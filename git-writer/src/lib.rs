//! Shared handcrafted Git bare-repository writer used by data compilers.
//!
//! The writer streams Git pack/index objects directly into a temporary bare
//! repository, writes `refs/heads/main`, and moves the temporary repository into
//! place only after every object has been finalized.

use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process;

use anyhow::{Context, Result, anyhow, bail};
use crc32fast::Hasher as Crc32Hasher;
use sha1::{Digest, Sha1};
use smallvec::SmallVec;

/// Supported pack entry kinds emitted by the handcrafted writer.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PackObjectKind {
    /// Full commit object payload.
    Commit = 1,
    /// Full tree object payload.
    Tree = 2,
    /// Full blob object payload.
    Blob = 3,
    /// Delta payload that references a base by negative offset.
    OfsDelta = 6,
}

impl PackObjectKind {
    /// Returns the Git object header name.
    fn git_type_name(self) -> &'static [u8] {
        match self {
            Self::Commit => b"commit",
            Self::Tree => b"tree",
            Self::Blob => b"blob",
            Self::OfsDelta => {
                panic!("delta objects do not have standalone git object headers")
            }
        }
    }
}

/// Git identity pair used in handcrafted commit objects.
#[derive(Debug, Clone, Copy)]
struct GitPerson<'a> {
    /// Display name in the commit header.
    name: &'a str,
    /// Email address in the commit header.
    email: &'a str,
}

/// Author/committer identities paired for one handcrafted commit.
#[derive(Debug, Clone, Copy)]
struct CommitPeople<'a> {
    /// Author identity recorded in the commit body.
    author: GitPerson<'a>,
    /// Committer identity recorded in the commit body.
    committer: GitPerson<'a>,
}

/// Borrowed blob payload that was already hashed and compressed.
pub struct PreparedBlob<'a> {
    /// Original blob size.
    size: usize,
    /// Canonical Git object id for the blob body.
    sha: [u8; 20],
    /// Deflated PACK payload for the blob body.
    compressed: &'a [u8],
}

impl<'a> PreparedBlob<'a> {
    /// Wraps a precomputed blob id and pack payload for one file body.
    pub fn from_parts(content: &[u8], sha: [u8; 20], compressed: &'a [u8]) -> Self {
        Self {
            size: content.len(),
            sha,
            compressed,
        }
    }
}

/// Commit timestamp rendered in Korea Standard Time (`+0900`).
#[derive(Debug, Clone, Copy)]
pub struct GitTimestampKst {
    /// Unix timestamp in seconds.
    epoch: i64,
}

impl GitTimestampKst {
    /// Wraps an already-normalized Unix timestamp.
    pub fn from_epoch(epoch: i64) -> Self {
        Self { epoch }
    }
}

/// Precomputes the canonical blob id and compressed pack payload for one file body.
pub fn precompute_blob(content: &[u8]) -> ([u8; 20], Vec<u8>) {
    (
        git_hash(PackObjectKind::Blob.git_type_name(), content),
        compress(content),
    )
}

/// Owned repository path rendered as a slash-separated Git path.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RepoPathBuf {
    /// Slash-separated path relative to the repository root.
    path: String,
}

impl RepoPathBuf {
    /// Creates a root-level repository path.
    pub fn root_file(name: impl Into<String>) -> Self {
        Self { path: name.into() }
    }

    /// Creates a law Markdown path under `kr/<group>/`.
    pub fn kr_file(group: impl Into<String>, filename: impl Into<String>) -> Self {
        Self {
            path: format!("kr/{}/{}", group.into(), filename.into()),
        }
    }

    /// Creates a precedent Markdown path under `{case_type}/{court_tier}/`.
    pub fn prec_file(
        case_type: impl Into<String>,
        court_tier: impl Into<String>,
        filename: impl Into<String>,
    ) -> Self {
        Self {
            path: format!(
                "{}/{}/{}",
                case_type.into(),
                court_tier.into(),
                filename.into()
            ),
        }
    }

    /// Creates a generic slash-separated repository path.
    pub fn file(path: impl Into<String>) -> Self {
        Self { path: path.into() }
    }

    /// Borrows the rendered repository path.
    pub fn as_str(&self) -> &str {
        &self.path
    }
}

impl fmt::Display for RepoPathBuf {
    /// Renders the repository path in Git's slash-separated form.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.path)
    }
}

/// One in-memory tree entry.
#[derive(Debug)]
enum TreeEntry {
    /// Regular file with its current blob id.
    File {
        /// Blob object id.
        sha: [u8; 20],
    },
    /// Nested tree.
    Tree(TreeNode),
}

/// Mutable Git tree trie.
#[derive(Debug, Default)]
struct TreeNode {
    /// Child entries keyed by raw path component text.
    entries: BTreeMap<String, TreeEntry>,
    /// Most recently materialized tree id.
    cached_sha: Option<[u8; 20]>,
    /// Previous tree bytes kept as a delta base for repeated subtree updates.
    previous_tree_bytes: Option<Vec<u8>>,
    /// Pack byte offset of the previous tree object.
    previous_tree_offset: Option<u64>,
    /// Current tree delta chain depth.
    previous_tree_depth: usize,
}

impl TreeNode {
    /// Checks whether a file can be inserted without changing the tree.
    fn ensure_can_insert_file(&self, components: &[&str]) -> Result<()> {
        let Some((head, tail)) = components.split_first() else {
            bail!("empty repository path");
        };

        if tail.is_empty() {
            if matches!(self.entries.get(*head), Some(TreeEntry::Tree(_))) {
                bail!("path conflicts with existing tree: {head}");
            }
            return Ok(());
        }

        match self.entries.get(*head) {
            Some(TreeEntry::File { .. }) => bail!("path conflicts with existing file: {head}"),
            Some(TreeEntry::Tree(child)) => child.ensure_can_insert_file(tail),
            None => Ok(()),
        }
    }

    /// Inserts or updates one file entry, marking every ancestor tree dirty.
    fn insert_file(&mut self, components: &[&str], sha: [u8; 20]) -> Result<()> {
        let Some((head, tail)) = components.split_first() else {
            bail!("empty repository path");
        };
        self.cached_sha = None;

        if tail.is_empty() {
            match self.entries.get(*head) {
                Some(TreeEntry::Tree(_)) => bail!("path conflicts with existing tree: {head}"),
                Some(TreeEntry::File { .. }) | None => {
                    self.entries
                        .insert((*head).to_owned(), TreeEntry::File { sha });
                    Ok(())
                }
            }
        } else {
            let entry = self
                .entries
                .entry((*head).to_owned())
                .or_insert_with(|| TreeEntry::Tree(TreeNode::default()));
            match entry {
                TreeEntry::File { .. } => bail!("path conflicts with existing file: {head}"),
                TreeEntry::Tree(child) => child.insert_file(tail, sha),
            }
        }
    }

    /// Removes one file entry, pruning empty ancestor trees.
    fn remove_file(&mut self, components: &[&str]) -> Result<bool> {
        let Some((head, tail)) = components.split_first() else {
            bail!("empty repository path");
        };

        if tail.is_empty() {
            return match self.entries.get(*head) {
                Some(TreeEntry::File { .. }) => {
                    self.entries.remove(*head);
                    self.cached_sha = None;
                    Ok(true)
                }
                Some(TreeEntry::Tree(_)) => bail!("path conflicts with existing tree: {head}"),
                None => Ok(false),
            };
        }

        let mut prune_child = false;
        let removed = match self.entries.get_mut(*head) {
            Some(TreeEntry::File { .. }) => bail!("path conflicts with existing file: {head}"),
            Some(TreeEntry::Tree(child)) => {
                let removed = child.remove_file(tail)?;
                prune_child = removed && child.entries.is_empty();
                removed
            }
            None => false,
        };
        if removed {
            if prune_child {
                self.entries.remove(*head);
            }
            self.cached_sha = None;
        }
        Ok(removed)
    }

    /// Materializes this tree and every dirty child tree.
    fn materialize(&mut self, writer: &mut PackWriter) -> Result<[u8; 20]> {
        if let Some(sha) = self.cached_sha {
            return Ok(sha);
        }

        let mut entries = Vec::with_capacity(self.entries.len());
        for (name, entry) in &mut self.entries {
            match entry {
                TreeEntry::File { sha } => entries.push(SerializedTreeEntry {
                    is_tree: false,
                    name: name.as_bytes().to_vec(),
                    sha: *sha,
                }),
                TreeEntry::Tree(child) => {
                    let sha = child.materialize(writer)?;
                    entries.push(SerializedTreeEntry {
                        is_tree: true,
                        name: name.as_bytes().to_vec(),
                        sha,
                    });
                }
            }
        }
        entries.sort_by(git_tree_entry_cmp);

        let mut tree = Vec::new();
        for entry in entries {
            tree.extend_from_slice(if entry.is_tree { b"40000 " } else { b"100644 " });
            tree.extend_from_slice(&entry.name);
            tree.push(0);
            tree.extend_from_slice(&entry.sha);
        }

        let sha = git_hash(PackObjectKind::Tree.git_type_name(), &tree);
        let mut tree_depth = 0;
        if let (Some(previous_tree), Some(previous_offset)) =
            (&self.previous_tree_bytes, self.previous_tree_offset)
        {
            if self.previous_tree_depth < MAX_DELTA_DEPTH {
                let delta = create_delta(previous_tree, &tree);
                if delta.len() < tree.len() * 3 / 4 {
                    writer.write_ofs_delta(previous_offset, &delta, sha)?;
                    tree_depth = self.previous_tree_depth + 1;
                } else {
                    writer.write_object(PackObjectKind::Tree, &tree)?;
                }
            } else {
                writer.write_object(PackObjectKind::Tree, &tree)?;
            }
        } else {
            writer.write_object(PackObjectKind::Tree, &tree)?;
        }
        let offset = writer.object_offset(&sha)?;
        self.previous_tree_bytes = Some(tree);
        self.previous_tree_offset = Some(offset);
        self.previous_tree_depth = tree_depth;
        self.cached_sha = Some(sha);
        Ok(sha)
    }
}

/// Serialized tree entry metadata used for Git ordering.
struct SerializedTreeEntry {
    /// Whether this entry points at a tree.
    is_tree: bool,
    /// Raw tree entry name bytes.
    name: Vec<u8>,
    /// Target object id.
    sha: [u8; 20],
}

/// Compares tree entries using Git's directory-aware ordering.
fn git_tree_entry_cmp(left: &SerializedTreeEntry, right: &SerializedTreeEntry) -> Ordering {
    let common = left.name.len().min(right.name.len());
    match left.name[..common].cmp(&right.name[..common]) {
        Ordering::Equal => {
            let left_tail = if left.is_tree { b'/' } else { 0 };
            let right_tail = if right.is_tree { b'/' } else { 0 };
            let left_next = left.name.get(common).copied().unwrap_or(left_tail);
            let right_next = right.name.get(common).copied().unwrap_or(right_tail);
            left_next.cmp(&right_next)
        }
        ordering => ordering,
    }
}

/// One entry in the pack index, accumulated during pack writing.
struct IdxEntry {
    /// Object id of the packed object.
    sha: [u8; 20],
    /// CRC-32 of the raw pack entry bytes.
    crc32: u32,
    /// Byte offset of the entry within the pack file.
    offset: u64,
}

/// Low-level writer that streams packfile entries directly to the final `.pack` file.
struct PackWriter {
    /// Buffered writer for the pack file.
    file: BufWriter<File>,
    /// Number of unique objects appended to the pack stream.
    object_count: u32,
    /// Filesystem path of the `.pack` file being written.
    path: PathBuf,
    /// Object ids already emitted, mapped to their byte offset inside the pack.
    seen: HashMap<[u8; 20], u64>,
    /// Accumulated index entries for `.idx` v2 generation.
    idx_entries: Vec<IdxEntry>,
    /// Running byte offset tracking how many bytes have been written so far.
    bytes_written: u64,
}

/// Writes a generated history into a fresh bare Git repository.
pub struct BareRepoWriter {
    /// Streaming pack writer used for all objects in the temporary repo.
    writer: PackWriter,
    /// Temporary bare repository path populated before the final rename.
    temp_output: PathBuf,
    /// Requested output path for the finished bare repository.
    final_output: PathBuf,
    /// Current root tree state.
    root: TreeNode,
    /// Parent commit id for the next handcrafted commit object.
    parent_commit: Option<[u8; 20]>,
}

impl BareRepoWriter {
    /// Creates a new temporary bare repository writer for the requested output path.
    pub fn create(output: &Path) -> Result<Self> {
        let final_output = output.to_path_buf();
        let temp_output = sibling_temp_path(output, "tmp")?;
        if temp_output.exists() {
            remove_path(&temp_output)?;
        }

        let parent = temp_output
            .parent()
            .context("temporary output path has no parent")?;
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;

        let pack_path = temp_output.join("objects/pack/tmp_pack.pack");
        fs::create_dir_all(
            pack_path
                .parent()
                .context("pack path unexpectedly missing parent")?,
        )?;

        Ok(Self {
            writer: PackWriter::new(&pack_path)?,
            temp_output,
            final_output,
            root: TreeNode::default(),
            parent_commit: None,
        })
    }

    /// Commits one rendered generated Markdown file using bot authorship.
    pub fn commit_bot_file(
        &mut self,
        path: &RepoPathBuf,
        markdown: &[u8],
        blob_sha: [u8; 20],
        compressed_blob: &[u8],
        message: &str,
        time: GitTimestampKst,
    ) -> Result<()> {
        let bot = GitPerson {
            name: "legalize-kr-bot",
            email: "bot@legalize.kr",
        };
        self.commit_file(
            path,
            PreparedBlob::from_parts(markdown, blob_sha, compressed_blob),
            message,
            CommitPeople {
                author: bot,
                committer: bot,
            },
            time,
        )
    }

    /// Commits one rendered file while removing stale paths in the same commit.
    pub fn commit_bot_file_with_deletions(
        &mut self,
        path: &RepoPathBuf,
        blob: PreparedBlob<'_>,
        stale_paths: &[RepoPathBuf],
        message: &str,
        time: GitTimestampKst,
    ) -> Result<()> {
        let bot = GitPerson {
            name: "legalize-kr-bot",
            email: "bot@legalize.kr",
        };
        self.commit_file_with_deletions(
            path,
            blob,
            stale_paths,
            message,
            CommitPeople {
                author: bot,
                committer: bot,
            },
            time,
        )
    }

    /// Alias for law-like generated entries.
    pub fn commit_law(
        &mut self,
        path: &RepoPathBuf,
        markdown: &[u8],
        blob_sha: [u8; 20],
        compressed_blob: &[u8],
        message: &str,
        time: GitTimestampKst,
    ) -> Result<()> {
        self.commit_bot_file(path, markdown, blob_sha, compressed_blob, message, time)
    }

    /// Alias for precedent-like generated entries.
    pub fn commit_precedent(
        &mut self,
        path: &RepoPathBuf,
        markdown: &[u8],
        blob_sha: [u8; 20],
        compressed_blob: &[u8],
        message: &str,
        time: GitTimestampKst,
    ) -> Result<()> {
        self.commit_bot_file(path, markdown, blob_sha, compressed_blob, message, time)
    }

    /// Commits a static repository file with the fixed initial authorship metadata.
    pub fn commit_static(
        &mut self,
        path: &RepoPathBuf,
        content: &[u8],
        message: &str,
        epoch: i64,
    ) -> Result<()> {
        let author = GitPerson {
            name: "Junghwan Park",
            email: "reserve.dev@gmail.com",
        };
        let (blob_sha, compressed_blob) = precompute_blob(content);
        self.commit_file(
            path,
            PreparedBlob::from_parts(content, blob_sha, &compressed_blob),
            message,
            CommitPeople {
                author,
                committer: author,
            },
            GitTimestampKst::from_epoch(epoch),
        )
    }

    /// Finalizes the pack, writes `main` as loose refs, and moves the temporary repo into place.
    ///
    /// Returns the HEAD commit SHA of the finished repository, or `[0u8; 20]` when no commits
    /// were ever appended.
    pub fn finish(mut self) -> Result<[u8; 20]> {
        self.writer.finish()?;

        if let Some(parent_commit) = self.parent_commit {
            let refs_heads = self.temp_output.join("refs/heads");
            fs::create_dir_all(&refs_heads)
                .with_context(|| format!("failed to create {}", refs_heads.display()))?;
            fs::write(
                refs_heads.join("main"),
                format!("{}\n", hex(&parent_commit)),
            )
            .with_context(|| format!("failed to write {}", refs_heads.join("main").display()))?;
        }
        fs::write(self.temp_output.join("HEAD"), "ref: refs/heads/main\n").with_context(|| {
            format!(
                "failed to write {}",
                self.temp_output.join("HEAD").display()
            )
        })?;

        replace_path(&self.temp_output, &self.final_output)?;
        Ok(self.parent_commit.unwrap_or([0u8; 20]))
    }

    /// Commits one file change after updating blob and tree state.
    fn commit_file(
        &mut self,
        path: &RepoPathBuf,
        blob: PreparedBlob<'_>,
        message: &str,
        people: CommitPeople<'_>,
        time: GitTimestampKst,
    ) -> Result<()> {
        let components = validate_path(path.as_str())?;
        self.root.ensure_can_insert_file(&components)?;
        self.writer.write_precompressed_object(
            PackObjectKind::Blob,
            blob.size,
            blob.sha,
            blob.compressed,
        )?;
        self.root.insert_file(&components, blob.sha)?;
        let root_sha = self.root.materialize(&mut self.writer)?;
        let commit_sha =
            self.write_commit(root_sha, message, people.author, people.committer, time)?;
        self.parent_commit = Some(commit_sha);
        Ok(())
    }

    /// Commits one file change after deleting stale paths from the same tree.
    fn commit_file_with_deletions(
        &mut self,
        path: &RepoPathBuf,
        blob: PreparedBlob<'_>,
        stale_paths: &[RepoPathBuf],
        message: &str,
        people: CommitPeople<'_>,
        time: GitTimestampKst,
    ) -> Result<()> {
        let components = validate_path(path.as_str())?;
        let stale_components = stale_paths
            .iter()
            .map(|path| validate_path(path.as_str()))
            .collect::<Result<Vec<_>>>()?;
        self.root.ensure_can_insert_file(&components)?;
        for components in &stale_components {
            self.root.remove_file(components)?;
        }
        self.writer.write_precompressed_object(
            PackObjectKind::Blob,
            blob.size,
            blob.sha,
            blob.compressed,
        )?;
        self.root.insert_file(&components, blob.sha)?;
        let root_sha = self.root.materialize(&mut self.writer)?;
        let commit_sha =
            self.write_commit(root_sha, message, people.author, people.committer, time)?;
        self.parent_commit = Some(commit_sha);
        Ok(())
    }

    /// Serializes and appends one commit object to the pack stream.
    fn write_commit(
        &mut self,
        tree: [u8; 20],
        message: &str,
        author: GitPerson<'_>,
        committer: GitPerson<'_>,
        time: GitTimestampKst,
    ) -> Result<[u8; 20]> {
        use std::fmt::Write as _;
        let mut commit = String::with_capacity(1000);
        let tree_hex = hex_buf(&tree);
        let tree_hex_str = std::str::from_utf8(&tree_hex).unwrap();
        writeln!(commit, "tree {tree_hex_str}").unwrap();
        if let Some(parent) = self.parent_commit {
            let parent_hex = hex_buf(&parent);
            let parent_hex_str = std::str::from_utf8(&parent_hex).unwrap();
            writeln!(commit, "parent {parent_hex_str}").unwrap();
        }
        write!(
            commit,
            "author {} <{}> {} +0900\ncommitter {} <{}> {} +0900\n\n{message}",
            author.name, author.email, time.epoch, committer.name, committer.email, time.epoch
        )
        .unwrap();
        self.writer
            .write_object(PackObjectKind::Commit, commit.as_bytes())
    }
}

impl PackWriter {
    /// Creates a new pack writer that writes directly to the temporary `.pack` file.
    fn new(path: &Path) -> Result<Self> {
        let mut file = BufWriter::with_capacity(4 << 20, File::create(path)?);
        let pack_header: [u8; 12] = [b'P', b'A', b'C', b'K', 0, 0, 0, 2, 0, 0, 0, 0];
        file.write_all(&pack_header)?;
        Ok(Self {
            file,
            object_count: 0,
            path: path.to_path_buf(),
            seen: HashMap::new(),
            idx_entries: Vec::new(),
            bytes_written: 12,
        })
    }

    /// Appends one full object to the pack unless it was already emitted.
    fn write_object(&mut self, object_type: PackObjectKind, data: &[u8]) -> Result<[u8; 20]> {
        let sha = git_hash(object_type.git_type_name(), data);
        self.write_precompressed_object(object_type, data.len(), sha, &compress(data))
    }

    /// Appends one `OFS_DELTA` object to the pack unless the result id already exists.
    fn write_ofs_delta(
        &mut self,
        base_offset: u64,
        delta: &[u8],
        result_sha: [u8; 20],
    ) -> Result<[u8; 20]> {
        if self.seen.contains_key(&result_sha) {
            return Ok(result_sha);
        }

        let offset = self.bytes_written;
        self.seen.insert(result_sha, offset);
        let header_bytes = encode_pack_entry_header(PackObjectKind::OfsDelta, delta.len());
        let ofs_bytes = encode_ofs_delta_offset(offset - base_offset);
        let compressed = compress(delta);

        let mut crc = Crc32Hasher::new();
        crc.update(&header_bytes);
        crc.update(&ofs_bytes);
        crc.update(&compressed);

        self.file.write_all(&header_bytes)?;
        self.file.write_all(&ofs_bytes)?;
        self.file.write_all(&compressed)?;
        self.bytes_written +=
            header_bytes.len() as u64 + ofs_bytes.len() as u64 + compressed.len() as u64;
        self.object_count += 1;
        self.idx_entries.push(IdxEntry {
            sha: result_sha,
            crc32: crc.finalize(),
            offset,
        });
        Ok(result_sha)
    }

    /// Appends one full object whose object id and compressed payload were prepared earlier.
    fn write_precompressed_object(
        &mut self,
        object_type: PackObjectKind,
        size: usize,
        sha: [u8; 20],
        compressed: &[u8],
    ) -> Result<[u8; 20]> {
        if self.seen.contains_key(&sha) {
            return Ok(sha);
        }

        let offset = self.bytes_written;
        self.seen.insert(sha, offset);
        let header_bytes = encode_pack_entry_header(object_type, size);

        let mut crc = Crc32Hasher::new();
        crc.update(&header_bytes);
        crc.update(compressed);

        self.file.write_all(&header_bytes)?;
        self.file.write_all(compressed)?;
        self.bytes_written += header_bytes.len() as u64 + compressed.len() as u64;
        self.object_count += 1;
        self.idx_entries.push(IdxEntry {
            sha,
            crc32: crc.finalize(),
            offset,
        });
        Ok(sha)
    }

    /// Returns the pack byte offset for an already emitted object.
    fn object_offset(&self, sha: &[u8; 20]) -> Result<u64> {
        self.seen
            .get(sha)
            .copied()
            .ok_or_else(|| anyhow!("object was not written to the pack: {}", hex(sha)))
    }

    /// Finalizes the pack file and writes the matching `.idx` v2 index.
    fn finish(&mut self) -> Result<()> {
        self.file.flush()?;

        let inner = self.file.get_mut();
        inner.seek(SeekFrom::Start(8))?;
        inner.write_all(&self.object_count.to_be_bytes())?;
        inner.flush()?;

        let mut reader = BufReader::with_capacity(4 << 20, File::open(&self.path)?);
        let mut hasher = Sha1::new();
        let mut buffer = [0u8; 1 << 20];
        loop {
            let n = reader.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            hasher.update(&buffer[..n]);
        }
        drop(reader);
        let pack_checksum: [u8; 20] = hasher.finalize().into();

        let mut pack_file = fs::OpenOptions::new().append(true).open(&self.path)?;
        pack_file.write_all(&pack_checksum)?;
        pack_file.flush()?;
        drop(pack_file);

        self.write_idx_v2(&pack_checksum)?;

        let checksum_hex = hex(&pack_checksum);
        let pack_dir = self.path.parent().context("pack path has no parent")?;
        let final_pack = pack_dir.join(format!("pack-{checksum_hex}.pack"));
        let final_idx = pack_dir.join(format!("pack-{checksum_hex}.idx"));
        let tmp_idx = self.path.with_extension("idx");
        fs::rename(&self.path, &final_pack)?;
        fs::rename(&tmp_idx, &final_idx)?;
        Ok(())
    }

    /// Writes the `.idx` v2 index file alongside the pack.
    fn write_idx_v2(&mut self, pack_checksum: &[u8; 20]) -> Result<()> {
        self.idx_entries.sort_unstable_by_key(|entry| entry.sha);

        let idx_path = self.path.with_extension("idx");
        let mut f = BufWriter::with_capacity(4 << 20, File::create(&idx_path)?);
        let mut hasher = Sha1::new();

        let mut write = |data: &[u8]| -> Result<()> {
            f.write_all(data)?;
            hasher.update(data);
            Ok(())
        };

        write(&[0xff, 0x74, 0x4f, 0x63])?;
        write(&[0x00, 0x00, 0x00, 0x02])?;

        let mut fanout = [0u32; 256];
        for entry in &self.idx_entries {
            fanout[entry.sha[0] as usize] += 1;
        }
        for i in 1..256 {
            fanout[i] += fanout[i - 1];
        }
        for count in &fanout {
            write(&count.to_be_bytes())?;
        }

        for entry in &self.idx_entries {
            write(&entry.sha)?;
        }

        for entry in &self.idx_entries {
            write(&entry.crc32.to_be_bytes())?;
        }

        let mut large_offsets = Vec::new();
        for entry in &self.idx_entries {
            if entry.offset >= 0x8000_0000 {
                let large_idx = large_offsets.len() as u32;
                write(&(large_idx | 0x8000_0000).to_be_bytes())?;
                large_offsets.push(entry.offset);
            } else {
                write(&(entry.offset as u32).to_be_bytes())?;
            }
        }

        for &off in &large_offsets {
            write(&off.to_be_bytes())?;
        }

        write(pack_checksum)?;

        f.flush()?;
        let idx_checksum: [u8; 20] = hasher.finalize().into();
        f.write_all(&idx_checksum)?;
        f.flush()?;
        Ok(())
    }
}

/// Validates and splits a repository path into Git tree components.
fn validate_path(path: &str) -> Result<Vec<&str>> {
    if path.is_empty() {
        bail!("repository path is empty");
    }
    if path.as_bytes().contains(&0) {
        bail!("repository path contains NUL: {path:?}");
    }
    let components: Vec<&str> = path.split('/').collect();
    if components.iter().any(|component| component.is_empty()) {
        bail!("repository path contains an empty component: {path:?}");
    }
    Ok(components)
}

/// Encodes the variable-length PACK entry header into a stack buffer.
#[inline]
fn encode_pack_entry_header(object_type: PackObjectKind, size: usize) -> SmallVec<[u8; 16]> {
    let mut buf = SmallVec::new();
    let mut header = ((object_type as u8 & 0b111) << 4) | (size as u8 & 0x0f);
    let mut remaining = size >> 4;
    if remaining > 0 {
        header |= 0x80;
    }
    buf.push(header);
    while remaining > 0 {
        let mut byte = (remaining & 0x7f) as u8;
        remaining >>= 7;
        if remaining > 0 {
            byte |= 0x80;
        }
        buf.push(byte);
    }
    buf
}

/// Encodes the negative offset for an `OFS_DELTA` pack entry.
#[inline]
fn encode_ofs_delta_offset(mut offset: u64) -> SmallVec<[u8; 16]> {
    let mut buf = SmallVec::<[u8; 16]>::new();
    buf.push((offset & 0x7f) as u8);
    offset >>= 7;
    while offset > 0 {
        offset -= 1;
        buf.push(0x80 | (offset & 0x7f) as u8);
        offset >>= 7;
    }
    buf.reverse();
    buf
}

thread_local! {
    /// Reusable scratch buffer for compression output to avoid per-call allocation.
    static COMP_BUF: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };

    /// Reuses one fast zlib compressor per thread for whole-buffer pack payload compression.
    #[cfg(feature = "libdeflater")]
    static COMPRESSOR: RefCell<libdeflater::Compressor> =
        RefCell::new(libdeflater::Compressor::new(libdeflater::CompressionLvl::new(1).unwrap()));
}

/// Compresses one pack payload with the current fast zlib setting.
fn compress(data: &[u8]) -> Vec<u8> {
    COMP_BUF.with(|buf_cell| {
        #[cfg(feature = "libdeflater")]
        return COMPRESSOR.with(|comp_cell| {
            let mut comp = comp_cell.borrow_mut();
            let mut buf = buf_cell.borrow_mut();
            let bound = comp.zlib_compress_bound(data.len());
            buf.resize(bound, 0);
            let actual = comp
                .zlib_compress(data, &mut buf)
                .expect("zlib_compress_bound() must allocate enough space");
            buf[..actual].to_vec()
        });

        #[cfg(not(feature = "libdeflater"))]
        {
            use zlib_rs::{DeflateConfig, ReturnCode, compress_bound, compress_slice};

            let mut buf = buf_cell.borrow_mut();
            buf.resize(compress_bound(data.len()), 0);
            let (compressed, rc) = compress_slice(&mut buf, data, DeflateConfig::new(1));
            assert_eq!(rc, ReturnCode::Ok);
            compressed.to_vec()
        }
    })
}

/// Fixed block width used by the delta matcher.
const DELTA_BLOCK_SIZE: usize = 16;

/// Maximum tree delta chain depth.
const MAX_DELTA_DEPTH: usize = 50;

/// Builds a Git copy/insert delta from `src` to `dst`.
#[inline(never)]
fn create_delta(src: &[u8], dst: &[u8]) -> Vec<u8> {
    let mut delta = Vec::with_capacity(dst.len() / 2);
    encode_varint(&mut delta, src.len());
    encode_varint(&mut delta, dst.len());

    if src.len() < DELTA_BLOCK_SIZE {
        emit_inserts(&mut delta, dst);
        return delta;
    }

    let (source_blocks, _) = src.as_chunks();
    let source_block_count = source_blocks.len();
    let mut index: HashMap<u32, SmallVec<[usize; 4]>> = HashMap::with_capacity(source_block_count);
    for (block_index, block) in source_blocks.iter().enumerate() {
        index
            .entry(block_hash(block))
            .or_default()
            .push(block_index * DELTA_BLOCK_SIZE);
    }

    let mut destination_offset = 0usize;
    let mut pending = Vec::new();

    while destination_offset < dst.len() {
        let mut best_source_offset = 0usize;
        let mut best_len = 0usize;

        if let Some(block) = dst[destination_offset..].first_chunk() {
            let hash = block_hash(block);
            if let Some(candidates) = index.get(&hash) {
                for &source_offset in candidates {
                    let match_len = match_length(&src[source_offset..], &dst[destination_offset..]);
                    if match_len > best_len {
                        best_len = match_len;
                        best_source_offset = source_offset;
                    }
                }
            }
        }

        if best_len >= DELTA_BLOCK_SIZE {
            flush_inserts(&mut delta, &mut pending);
            emit_copy(&mut delta, best_source_offset, best_len);
            destination_offset += best_len;
        } else {
            pending.push(dst[destination_offset]);
            destination_offset += 1;
        }
    }

    flush_inserts(&mut delta, &mut pending);
    delta
}

/// Emits one Git delta copy instruction.
fn emit_copy(out: &mut Vec<u8>, offset: usize, size: usize) {
    let mut command = 0x80;
    let mut args = [0_u8; 7];
    let mut arg_len = 0usize;
    let mut push_arg = |byte| {
        args[arg_len] = byte;
        arg_len += 1;
    };
    if offset & 0xff != 0 {
        command |= 0x01;
        push_arg((offset & 0xff) as u8);
    }
    if offset & 0xff00 != 0 {
        command |= 0x02;
        push_arg(((offset >> 8) & 0xff) as u8);
    }
    if offset & 0xff0000 != 0 {
        command |= 0x04;
        push_arg(((offset >> 16) & 0xff) as u8);
    }
    if offset & 0xff000000 != 0 {
        command |= 0x08;
        push_arg(((offset >> 24) & 0xff) as u8);
    }
    if size & 0xff != 0 {
        command |= 0x10;
        push_arg((size & 0xff) as u8);
    }
    if size & 0xff00 != 0 {
        command |= 0x20;
        push_arg(((size >> 8) & 0xff) as u8);
    }
    if size & 0xff0000 != 0 {
        command |= 0x40;
        push_arg(((size >> 16) & 0xff) as u8);
    }
    out.push(command);
    out.extend_from_slice(&args[..arg_len]);
}

/// Emits literal insert commands, chunked to Git's 127-byte opcode limit.
fn emit_inserts(out: &mut Vec<u8>, data: &[u8]) {
    let mut offset = 0usize;
    while offset < data.len() {
        let chunk_len = std::cmp::min(127, data.len() - offset);
        out.push(chunk_len as u8);
        out.extend_from_slice(&data[offset..offset + chunk_len]);
        offset += chunk_len;
    }
}

/// Flushes buffered literal bytes into insert commands.
fn flush_inserts(out: &mut Vec<u8>, pending: &mut Vec<u8>) {
    if !pending.is_empty() {
        emit_inserts(out, pending);
        pending.clear();
    }
}

/// Encodes one Git-style little-endian base-128 integer.
fn encode_varint(out: &mut Vec<u8>, mut value: usize) {
    while value >= 128 {
        out.push((value & 0x7f) as u8 | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

/// Hashes one fixed-size block for delta index lookup.
fn block_hash(data: &[u8; DELTA_BLOCK_SIZE]) -> u32 {
    let left = u64::from_ne_bytes(data[..8].try_into().unwrap());
    let right = u64::from_ne_bytes(data[8..].try_into().unwrap());
    let mixed = left.wrapping_mul(0x9e37_79b1_85eb_ca87)
        ^ right.rotate_left(23).wrapping_mul(0xc2b2_ae3d_27d4_eb4f);
    (mixed ^ (mixed >> 32)) as u32
}

/// Returns the byte length of the common run starting at the two offsets.
#[inline(always)]
fn match_length(src: &[u8], dst: &[u8]) -> usize {
    const WORD_SIZE: usize = std::mem::size_of::<usize>();

    let max = std::cmp::min(src.len(), dst.len());

    let mut len = 0usize;
    while len + WORD_SIZE <= max {
        let src_word = usize::from_ne_bytes(src[len..len + WORD_SIZE].try_into().unwrap());
        let dst_word = usize::from_ne_bytes(dst[len..len + WORD_SIZE].try_into().unwrap());
        if src_word == dst_word {
            len += WORD_SIZE;
            continue;
        }

        let mismatch = src_word ^ dst_word;
        let mismatch_bits = if cfg!(target_endian = "little") {
            mismatch.trailing_zeros()
        } else {
            mismatch.leading_zeros()
        };
        return len + (mismatch_bits as usize / 8);
    }
    while len < max && src[len] == dst[len] {
        len += 1;
    }
    len
}

/// Computes the canonical Git object id for one unhashed object body.
fn git_hash(type_name: &[u8], data: &[u8]) -> [u8; 20] {
    let mut hasher = Sha1::new();
    let mut len_buf = [0_u8; 20];
    let mut cursor = len_buf.len();
    let mut value = data.len();
    loop {
        cursor -= 1;
        len_buf[cursor] = b'0' + (value % 10) as u8;
        value /= 10;
        if value == 0 {
            break;
        }
    }

    hasher.update(type_name);
    hasher.update(b" ");
    hasher.update(&len_buf[cursor..]);
    hasher.update([0]);
    hasher.update(data);
    hasher.finalize().into()
}

/// Stack-based hex encoding for the commit write hot path.
fn hex_buf(sha: &[u8; 20]) -> [u8; 40] {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut buf = [0u8; 40];
    for (index, &byte) in sha.iter().enumerate() {
        buf[index * 2] = HEX[(byte >> 4) as usize];
        buf[index * 2 + 1] = HEX[(byte & 0xf) as usize];
    }
    buf
}

/// Hex-encodes one object id for refs, logging, and non-hot-path usage.
pub fn hex(sha: &[u8; 20]) -> String {
    let buf = hex_buf(sha);
    String::from_utf8(buf.to_vec()).expect("hex digits are valid UTF-8")
}

/// Builds a temporary or backup path next to the final output path.
fn sibling_temp_path(output: &Path, suffix: &str) -> Result<PathBuf> {
    let parent = output.parent().unwrap_or_else(|| Path::new("."));
    let name = output
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| anyhow!("invalid output path: {}", output.display()))?;
    Ok(parent.join(format!(".{name}.{suffix}-{}", process::id())))
}

/// Deletes a file or directory tree at `path`.
fn remove_path(path: &Path) -> Result<()> {
    let metadata =
        fs::symlink_metadata(path).with_context(|| format!("failed to read {}", path.display()))?;
    if metadata.is_dir() {
        fs::remove_dir_all(path).with_context(|| format!("failed to remove {}", path.display()))?;
    } else {
        fs::remove_file(path).with_context(|| format!("failed to remove {}", path.display()))?;
    }
    Ok(())
}

/// Replaces `final_path` with `temp_path`, preserving the old output until the final rename.
fn replace_path(temp_path: &Path, final_path: &Path) -> Result<()> {
    if !final_path.exists() {
        fs::rename(temp_path, final_path).with_context(|| {
            format!(
                "failed to move {} to {}",
                temp_path.display(),
                final_path.display()
            )
        })?;
        return Ok(());
    }

    let backup_path = sibling_temp_path(final_path, "old")?;
    if backup_path.exists() {
        remove_path(&backup_path)?;
    }
    fs::rename(final_path, &backup_path).with_context(|| {
        format!(
            "failed to move existing {} to {}",
            final_path.display(),
            backup_path.display()
        )
    })?;

    if let Err(error) = fs::rename(temp_path, final_path) {
        let restore_result = fs::rename(&backup_path, final_path);
        if let Err(restore_error) = restore_result {
            bail!(
                "failed to move {} to {}: {error}; additionally failed to restore {}: {restore_error}",
                temp_path.display(),
                final_path.display(),
                backup_path.display()
            );
        }
        return Err(error).with_context(|| {
            format!(
                "failed to move {} to {}",
                temp_path.display(),
                final_path.display()
            )
        });
    }

    remove_path(&backup_path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;
    use std::process::{Command, Output};

    use tempfile::TempDir;

    use super::*;

    /// Creates a Git command with user config disabled for deterministic behavior.
    fn git_command() -> Command {
        let mut command = Command::new("git");
        command.env("GIT_CONFIG_GLOBAL", "/dev/null");
        command.env("GIT_CONFIG_NOSYSTEM", "1");
        command.env_remove("GIT_DIR");
        command.env_remove("GIT_WORK_TREE");
        command
    }

    /// Converts a failed Git subprocess result into a rich error.
    fn ensure_command_success(output: Output, context: &str) -> Result<()> {
        if output.status.success() {
            return Ok(());
        }

        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_owned();
        bail!(
            "{context}: exit status {}{}{}",
            output.status,
            if stderr.is_empty() { "" } else { "\nstderr:\n" },
            if stderr.is_empty() {
                String::new()
            } else if stdout.is_empty() {
                stderr
            } else {
                format!("{stderr}\nstdout:\n{stdout}")
            }
        )
    }

    #[test]
    fn builds_cloneable_deep_tree_repo() {
        let temp = TempDir::new().unwrap();
        let output = temp.path().join("output.git");
        let mut writer = BareRepoWriter::create(&output).unwrap();
        writer
            .commit_static(
                &RepoPathBuf::root_file("README.md"),
                b"hello\n",
                "initial commit",
                1_774_839_600,
            )
            .unwrap();
        let body = b"body\n";
        let (sha, compressed) = precompute_blob(body);
        writer
            .commit_bot_file(
                &RepoPathBuf::file("a/b/c/d/본문.md"),
                body,
                sha,
                &compressed,
                "data commit",
                GitTimestampKst::from_epoch(1_704_067_200),
            )
            .unwrap();
        let head_sha = writer.finish().unwrap();

        git_ok(&output, ["fsck", "--full"]);
        assert_eq!(
            hex(&head_sha),
            git_stdout(&output, ["rev-parse", "HEAD"]).trim()
        );
        assert_eq!(
            git_stdout(&output, ["rev-list", "--count", "HEAD"]).trim(),
            "2"
        );
        assert_eq!(
            git_stdout(&output, ["show", "HEAD:a/b/c/d/본문.md"]),
            "body\n"
        );

        let clone = temp.path().join("clone");
        let output = git_command()
            .arg("clone")
            .arg(&output)
            .arg(&clone)
            .output()
            .unwrap();
        ensure_command_success(output, "git clone").unwrap();
        assert_eq!(
            fs::read_to_string(clone.join("a/b/c/d/본문.md")).unwrap(),
            "body\n"
        );
    }

    #[test]
    fn updates_existing_file_path() {
        let temp = TempDir::new().unwrap();
        let output = temp.path().join("output.git");
        let mut writer = BareRepoWriter::create(&output).unwrap();
        for (message, content) in [
            ("first", b"one\n".as_slice()),
            ("second", b"two\n".as_slice()),
        ] {
            let (sha, compressed) = precompute_blob(content);
            writer
                .commit_bot_file(
                    &RepoPathBuf::file("same/path.md"),
                    content,
                    sha,
                    &compressed,
                    message,
                    GitTimestampKst::from_epoch(1),
                )
                .unwrap();
        }
        writer.finish().unwrap();

        git_ok(&output, ["fsck", "--full"]);
        assert_eq!(
            git_stdout(&output, ["rev-list", "--count", "HEAD"]).trim(),
            "2"
        );
        assert_eq!(git_stdout(&output, ["show", "HEAD:same/path.md"]), "two\n");
    }

    #[test]
    fn deletes_stale_path_in_same_commit() {
        let temp = TempDir::new().unwrap();
        let output = temp.path().join("output.git");
        let mut writer = BareRepoWriter::create(&output).unwrap();

        let old = b"old\n";
        let (sha, compressed) = precompute_blob(old);
        writer
            .commit_bot_file(
                &RepoPathBuf::file("old/path.md"),
                old,
                sha,
                &compressed,
                "old",
                GitTimestampKst::from_epoch(1),
            )
            .unwrap();

        let new = b"new\n";
        let (sha, compressed) = precompute_blob(new);
        writer
            .commit_bot_file_with_deletions(
                &RepoPathBuf::file("new/path.md"),
                PreparedBlob::from_parts(new, sha, &compressed),
                &[RepoPathBuf::file("old/path.md")],
                "rename",
                GitTimestampKst::from_epoch(2),
            )
            .unwrap();
        writer.finish().unwrap();

        git_ok(&output, ["fsck", "--full"]);
        assert_eq!(
            git_stdout(&output, ["rev-list", "--count", "HEAD"]).trim(),
            "2"
        );
        assert_eq!(git_stdout(&output, ["show", "HEAD:new/path.md"]), "new\n");
        assert!(
            git_stdout(&output, ["ls-tree", "-r", "--name-only", "HEAD"]).contains("new/path.md")
        );
        assert!(
            !git_stdout(&output, ["ls-tree", "-r", "--name-only", "HEAD"]).contains("old/path.md")
        );
    }

    #[test]
    fn rejects_invalid_paths_before_finish() {
        let temp = TempDir::new().unwrap();
        let output = temp.path().join("output.git");
        let mut writer = BareRepoWriter::create(&output).unwrap();
        let (sha, compressed) = precompute_blob(b"body");
        let error = writer
            .commit_bot_file(
                &RepoPathBuf::file("bad//path.md"),
                b"body",
                sha,
                &compressed,
                "bad",
                GitTimestampKst::from_epoch(1),
            )
            .unwrap_err();
        assert!(error.to_string().contains("empty component"));
    }

    #[test]
    fn rejects_path_conflicts_before_writing_blob() {
        let temp = TempDir::new().unwrap();
        let output = temp.path().join("output.git");
        let mut writer = BareRepoWriter::create(&output).unwrap();
        let body = b"body\n";
        let (sha, compressed) = precompute_blob(body);
        writer
            .commit_bot_file(
                &RepoPathBuf::file("same"),
                body,
                sha,
                &compressed,
                "first",
                GitTimestampKst::from_epoch(1),
            )
            .unwrap();

        let conflict = b"conflict\n";
        let (sha, compressed) = precompute_blob(conflict);
        let error = writer
            .commit_bot_file(
                &RepoPathBuf::file("same/path.md"),
                conflict,
                sha,
                &compressed,
                "conflict",
                GitTimestampKst::from_epoch(2),
            )
            .unwrap_err();
        assert!(error.to_string().contains("existing file"));

        writer.finish().unwrap();
        git_ok(&output, ["fsck", "--full"]);
        assert_eq!(
            git_stdout(&output, ["rev-list", "--count", "HEAD"]).trim(),
            "1"
        );
        assert!(git_stdout(&output, ["count-objects", "-v"]).contains("in-pack: 3\n"));
    }

    #[test]
    fn preserves_existing_output_until_finish() {
        let temp = TempDir::new().unwrap();
        let output = temp.path().join("output.git");
        fs::create_dir(&output).unwrap();
        fs::write(output.join("marker"), "keep").unwrap();

        let mut writer = BareRepoWriter::create(&output).unwrap();
        let body = b"body\n";
        let (sha, compressed) = precompute_blob(body);
        writer
            .commit_bot_file(
                &RepoPathBuf::file("path.md"),
                body,
                sha,
                &compressed,
                "commit",
                GitTimestampKst::from_epoch(1),
            )
            .unwrap();

        assert_eq!(fs::read_to_string(output.join("marker")).unwrap(), "keep");
        writer.finish().unwrap();

        assert!(!output.join("marker").exists());
        git_ok(&output, ["fsck", "--full"]);
        assert_eq!(git_stdout(&output, ["show", "HEAD:path.md"]), "body\n");
    }

    #[test]
    fn writes_tree_deltas_for_incremental_subtree_growth() {
        let temp = TempDir::new().unwrap();
        let output = temp.path().join("output.git");
        let mut writer = BareRepoWriter::create(&output).unwrap();

        for index in 0..80 {
            let content = format!("body {index}\n");
            let (sha, compressed) = precompute_blob(content.as_bytes());
            writer
                .commit_bot_file(
                    &RepoPathBuf::file(format!("group/subtree/{index:03}.md")),
                    content.as_bytes(),
                    sha,
                    &compressed,
                    "data commit",
                    GitTimestampKst::from_epoch(1 + index),
                )
                .unwrap();
        }
        writer.finish().unwrap();

        git_ok(&output, ["fsck", "--full"]);

        let idx_path = fs::read_dir(output.join("objects/pack"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| path.extension().is_some_and(|extension| extension == "idx"))
            .unwrap();
        let verify_pack = git_command()
            .arg("verify-pack")
            .arg("-v")
            .arg(idx_path)
            .output()
            .unwrap();
        let stdout = verify_pack.stdout.clone();
        ensure_command_success(verify_pack, "git verify-pack").unwrap();
        let stdout = String::from_utf8(stdout).unwrap();
        let tree_deltas = stdout
            .lines()
            .filter(|line| {
                let fields: Vec<_> = line.split_whitespace().collect();
                fields.get(1) == Some(&"tree") && fields.len() >= 7
            })
            .count();
        assert!(tree_deltas > 0, "expected at least one tree delta");
    }

    fn git_ok<const N: usize>(repo: &Path, args: [&str; N]) {
        let output = git_command()
            .arg("-C")
            .arg(repo)
            .args(args)
            .output()
            .unwrap();
        ensure_command_success(output, "git test helper").unwrap();
    }

    fn git_stdout<const N: usize>(repo: &Path, args: [&str; N]) -> String {
        let mut command = git_command();
        command.arg("-C").arg(repo);
        for arg in args {
            command.arg(arg);
        }

        let output = command.output().unwrap();
        let stdout = output.stdout.clone();
        ensure_command_success(output, "git test helper").unwrap();
        String::from_utf8(stdout).unwrap()
    }
}
