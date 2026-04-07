use std::cell::RefCell;
use std::fmt;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process;
#[cfg(test)]
use std::process::{Command, Output};

use anyhow::{Context, Result, anyhow, bail};
use libdeflater::{CompressionLvl, Compressor, Crc};
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};
use sha1::{Digest, Sha1};
use smallvec::SmallVec;
use time::{Date, Month, PrimitiveDateTime, Time as CivilTime, UtcOffset};

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
    /// Delta payload that references a base object id.
    RefDelta = 7,
}

impl PackObjectKind {
    /// Returns the Git object header name for full objects.
    fn git_type_name(self) -> &'static [u8] {
        match self {
            Self::Commit => b"commit",
            Self::Tree => b"tree",
            Self::Blob => b"blob",
            Self::RefDelta => panic!("ref deltas do not have standalone git object headers"),
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

/// Commit timestamp that is always rendered in Korea Standard Time (`+0900`).
#[derive(Debug, Clone, Copy)]
pub struct GitTimestampKst {
    /// Unix timestamp in seconds.
    epoch: i64,
}

impl GitTimestampKst {
    /// Converts a promulgation date into the deterministic noon-KST commit timestamp.
    pub fn from_promulgation_date(promulgation_date: &str) -> Result<Self> {
        //
        // Promulgation dates are expected to come from the XML cache as bare `YYYYMMDD`.
        // Reject anything else instead of quietly accepting alternate spellings.
        //
        if promulgation_date.len() != 8
            || !promulgation_date.bytes().all(|byte| byte.is_ascii_digit())
        {
            bail!("expected promulgation date in YYYYMMDD form: {promulgation_date}");
        }

        //
        // Clamp malformed inputs and pre-epoch dates before conversion so reruns keep producing the
        // same commit ids when upstream metadata predates Unix time.
        //
        let effective_date = if promulgation_date < "19700101" {
            String::from("1970-01-01")
        } else {
            format!(
                "{}-{}-{}",
                &promulgation_date[..4],
                &promulgation_date[4..6],
                &promulgation_date[6..8]
            )
        };

        //
        // Every revision commit lands at noon KST. The fixed wall-clock time keeps hashes stable
        // while still rendering as a readable calendar date in Git history.
        //
        let year = effective_date[0..4].parse::<i32>()?;
        let month = effective_date[5..7].parse::<u8>()?;
        let day = effective_date[8..10].parse::<u8>()?;
        let month = Month::try_from(month)?;
        let date = Date::from_calendar_date(year, month, day)?;
        let datetime = PrimitiveDateTime::new(date, CivilTime::from_hms(12, 0, 0)?);
        let offset = UtcOffset::from_hms(9, 0, 0)?;
        Ok(Self {
            epoch: datetime.assume_offset(offset).unix_timestamp(),
        })
    }
}

/// Precomputes the canonical blob id and compressed pack payload for one file body.
pub fn precompute_blob(content: &[u8]) -> ([u8; 20], Vec<u8>) {
    (
        git_hash(PackObjectKind::Blob.git_type_name(), content),
        compress(content),
    )
}

/// Owned repository path in either the root or `kr/<group>/` namespace.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RepoPathBuf {
    /// Root-level repository file such as `README.md`.
    RootFile(String),
    /// Law Markdown file under `kr/<group>/<filename>.md`.
    KrFile {
        /// Parent law directory name below `kr/`.
        group: String,
        /// Leaf Markdown filename inside the group directory.
        filename: String,
    },
}

impl RepoPathBuf {
    /// Creates a root-level repository path.
    pub fn root_file(name: impl Into<String>) -> Self {
        Self::RootFile(name.into())
    }

    /// Creates a law Markdown path under `kr/<group>/`.
    pub fn kr_file(group: impl Into<String>, filename: impl Into<String>) -> Self {
        Self::KrFile {
            group: group.into(),
            filename: filename.into(),
        }
    }
}

impl fmt::Display for RepoPathBuf {
    /// Renders the repository path in Git's slash-separated form.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RootFile(name) => f.write_str(name),
            Self::KrFile { group, filename } => write!(f, "kr/{group}/{filename}"),
        }
    }
}

/// One tree entry inside either the root tree or a law group subtree.
#[derive(Debug, Clone)]
struct Entry {
    /// Raw tree entry name bytes.
    name: Vec<u8>,
    /// Object id pointed to by the tree entry.
    sha: [u8; 20],
    /// Whether the target object is itself a tree.
    is_tree: bool,
    /// Previous blob revision kept as a delta base for repeated file updates.
    previous_blob: Option<PreviousBlob>,
}

/// Cached state for one `kr/<group>/` subtree.
#[derive(Debug, Clone)]
struct Group {
    /// Directory name below `kr/`.
    name: Vec<u8>,
    /// Sorted file entries inside that subtree.
    files: Vec<Entry>,
    /// Most recently materialized subtree SHA.
    cached_sha: Option<[u8; 20]>,
}

/// Previous blob revision kept as a possible delta base.
#[derive(Debug, Clone)]
struct PreviousBlob {
    /// Object id of the cached base blob.
    sha: [u8; 20],
    /// Full blob contents used for delta construction.
    content: Vec<u8>,
}

/// Borrowed blob payload that was already hashed and compressed off the writer hot path.
struct PrecomputedBlob<'a> {
    /// Canonical Git object id for the blob body.
    sha: [u8; 20],
    /// Deflated PACK payload for the blob body.
    compressed: &'a [u8],
}

/// Root-tree entry kinds that can be patched in-place in the cached bytes.
#[derive(Debug, Clone, Copy)]
enum DirtyRootEntry {
    /// One root-level file entry changed.
    File(usize),
    /// The `kr/` subtree entry changed.
    Kr,
}

/// Cached root-tree bytes plus patch metadata for repeated commits.
#[derive(Debug, Default)]
struct RootTreeState {
    /// Sorted root-level file entries.
    files: Vec<Entry>,
    /// Serialized root tree bytes reused across commits.
    cache: Vec<u8>,
    /// SHA byte offsets for each root file entry.
    sha_offsets: Vec<usize>,
    /// SHA byte offset for the optional `kr/` subtree entry.
    kr_sha_offset: Option<usize>,
    /// Most recent root-tree entry that changed.
    dirty_entry: Option<DirtyRootEntry>,
    /// Most recently written root tree SHA.
    current_sha: Option<[u8; 20]>,
}

/// Cached `kr/` subtree bytes plus per-group lookup metadata.
#[derive(Debug, Default)]
struct KrTreeState {
    /// Sorted law groups below `kr/`.
    groups: Vec<Group>,
    /// Fast lookup from group name to its position in `groups`.
    group_indices: HashMap<Vec<u8>, usize>,
    /// Serialized `kr/` tree bytes reused across commits.
    cache: Vec<u8>,
    /// SHA byte offsets for each group entry inside `cache`.
    sha_offsets: Vec<usize>,
    /// Most recently written `kr/` tree SHA.
    current_sha: Option<[u8; 20]>,
    /// Whether group insertion changed the `kr/` tree layout.
    structure_dirty: bool,
    /// Group index whose subtree SHA changed most recently.
    dirty_group_index: Option<usize>,
}

/// One entry in the pack index, accumulated during pack writing.
struct IdxEntry {
    /// Object id of the packed object.
    sha: [u8; 20],
    /// CRC-32 of the raw pack entry bytes (header + optional base SHA + compressed data).
    crc32: u32,
    /// Byte offset of the entry within the pack file.
    offset: u64,
}

/// Low-level writer that streams packfile entries directly to the final `.pack` file.
struct PackWriter {
    /// Buffered writer for the pack file (header already written).
    file: BufWriter<File>,
    /// Number of unique objects appended to the pack stream.
    object_count: u32,
    /// Filesystem path of the `.pack` file being written.
    path: PathBuf,
    /// Object ids already emitted into the pack stream.
    seen: HashSet<[u8; 20]>,
    /// Accumulated index entries for `.idx` v2 generation.
    idx_entries: Vec<IdxEntry>,
    /// Running byte offset tracking how many bytes have been written so far.
    bytes_written: u64,
}

/// Writes the generated law history into a fresh bare Git repository.
pub struct BareRepoWriter {
    /// Streaming pack writer used for all objects in the temporary repo.
    writer: PackWriter,
    /// Temporary bare repository path populated before the final rename.
    temp_output: PathBuf,
    /// Requested output path for the finished bare repository.
    final_output: PathBuf,

    // Root-level files plus the cached serialized root tree used for REF_DELTA updates.
    /// Root-tree cache and patch metadata.
    root: RootTreeState,

    // Subtree history that lets repeated law revisions reuse previous objects.
    /// `kr/` subtree cache and patch metadata.
    kr: KrTreeState,
    /// Parent commit id for the next handcrafted commit object.
    parent_commit: Option<[u8; 20]>,
    /// Whether a tree update must be materialized before the next commit.
    tree_dirty: bool,
}

impl BareRepoWriter {
    /// Creates a new temporary bare repository writer for the requested output path.
    pub fn create(output: &Path) -> Result<Self> {
        let final_output = output.to_path_buf();
        let temp_output = {
            let parent = output.parent().unwrap_or_else(|| Path::new("."));
            let name = output
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| anyhow!("invalid output path: {}", output.display()))?;
            parent.join(format!(".{name}.tmp-{}", process::id()))
        };
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
            root: RootTreeState::default(),
            kr: KrTreeState::default(),
            parent_commit: None,
            tree_dirty: false,
        })
    }

    /// Commits one rendered law Markdown file using bot authorship and law dates.
    pub fn commit_law(
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
        let blob = PrecomputedBlob {
            sha: blob_sha,
            compressed: compressed_blob,
        };
        self.commit_file(
            path,
            markdown,
            blob,
            message,
            CommitPeople {
                author: bot,
                committer: bot,
            },
            time,
        )
    }

    /// Commits a static repository file with the fixed initial authorship metadata.
    pub fn commit_static(
        &mut self,
        path: &RepoPathBuf,
        content: &[u8],
        message: &str,
        epoch: i64,
    ) -> Result<()> {
        let message = {
            let mut rendered = String::from(message.trim_end());
            rendered.push_str("\n\n");
            rendered.push_str("Co-authored-by: Jihyeon Kim <simnalamburt@gmail.com>");
            rendered
        };
        let author = GitPerson {
            name: "Junghwan Park",
            email: "reserve.dev@gmail.com",
        };
        let (blob_sha, compressed_blob) = precompute_blob(content);
        let blob = PrecomputedBlob {
            sha: blob_sha,
            compressed: &compressed_blob,
        };
        self.commit_file(
            path,
            content,
            blob,
            &message,
            CommitPeople {
                author,
                committer: author,
            },
            GitTimestampKst { epoch },
        )
    }

    /// Appends the empty historical contributor commit after the initial static files.
    pub fn commit_empty_initial_contributor(&mut self, message: &str, epoch: i64) -> Result<()> {
        if self.parent_commit.is_none() {
            bail!("empty contributor commit requires an existing tree");
        }
        let author = GitPerson {
            name: "Jihyeon Kim",
            email: "simnalamburt@gmail.com",
        };
        let root_sha = self.root_tree_sha()?;
        let commit_sha =
            self.write_commit(root_sha, message, author, author, GitTimestampKst { epoch })?;
        self.parent_commit = Some(commit_sha);
        Ok(())
    }

    /// Finalizes the pack, writes `main` as loose refs, and moves the temporary repo into place.
    pub fn finish(mut self) -> Result<()> {
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

        if self.final_output.exists() {
            remove_path(&self.final_output)?;
        }
        fs::rename(&self.temp_output, &self.final_output).with_context(|| {
            format!(
                "failed to move {} to {}",
                self.temp_output.display(),
                self.final_output.display()
            )
        })?;
        Ok(())
    }

    /// Commits one file change after updating blob and tree state.
    fn commit_file(
        &mut self,
        path: &RepoPathBuf,
        content: &[u8],
        blob: PrecomputedBlob<'_>,
        message: &str,
        people: CommitPeople<'_>,
        time: GitTimestampKst,
    ) -> Result<()> {
        // Warning: HOT PATH!

        //
        // Look up the touched file entry first so the previous blob body lives beside the tree
        // entry itself instead of behind a second global hash map keyed by RepoPathBuf.
        //
        let entry = match path {
            RepoPathBuf::RootFile(name) => {
                let (index, inserted) = upsert(&mut self.root.files, name.as_bytes(), false);
                if inserted {
                    self.root.sha_offsets.clear();
                    self.root.kr_sha_offset = None;
                    self.root.dirty_entry = None;
                } else {
                    self.root.dirty_entry = Some(DirtyRootEntry::File(index));
                }
                self.kr.dirty_group_index = None;
                &mut self.root.files[index]
            }
            RepoPathBuf::KrFile { group, filename } => {
                let group_index = self.ensure_group(group.as_bytes());
                let (index, _) = upsert(
                    &mut self.kr.groups[group_index].files,
                    filename.as_bytes(),
                    false,
                );
                self.kr.groups[group_index].cached_sha = None;
                self.kr.dirty_group_index = Some(group_index);
                self.root.dirty_entry = Some(DirtyRootEntry::Kr);
                &mut self.kr.groups[group_index].files[index]
            }
        };

        //
        // Store the file body first, preferably as a delta against the previous revision.
        //
        if let Some(previous) = entry.previous_blob.as_ref() {
            let previous_len = previous.content.len();
            let current_len = content.len();
            let (smaller, larger) = if previous_len <= current_len {
                (previous_len, current_len)
            } else {
                (current_len, previous_len)
            };

            //
            // Skip expensive delta construction for cases that almost never compress well:
            // identical blobs, very small bodies, or revisions whose sizes diverged too much.
            //
            if previous.sha != blob.sha && smaller >= 128 && larger <= smaller.saturating_mul(2) {
                let delta = create_delta(&previous.content, content);
                if delta.len() < content.len() * 3 / 4 {
                    self.writer
                        .write_ref_delta(previous.sha, &delta, blob.sha)?;
                } else {
                    self.writer.write_precompressed_object(
                        PackObjectKind::Blob,
                        content.len(),
                        blob.sha,
                        blob.compressed,
                    )?;
                }
            } else {
                self.writer.write_precompressed_object(
                    PackObjectKind::Blob,
                    content.len(),
                    blob.sha,
                    blob.compressed,
                )?;
            }
        } else {
            self.writer.write_precompressed_object(
                PackObjectKind::Blob,
                content.len(),
                blob.sha,
                blob.compressed,
            )?;
        }
        entry.sha = blob.sha;
        entry.previous_blob = Some(PreviousBlob {
            sha: blob.sha,
            content: content.to_vec(),
        });

        //
        // Materialize the current root tree and append the commit object in order.
        //
        self.tree_dirty = true;
        let root_sha = self.root_tree_sha()?;
        let commit_sha =
            self.write_commit(root_sha, message, people.author, people.committer, time)?;
        self.parent_commit = Some(commit_sha);
        Ok(())
    }

    /// Returns the stable sorted group slot for `kr/<group>/`, inserting it if needed.
    fn ensure_group(&mut self, name: &[u8]) -> usize {
        if let Some(&index) = self.kr.group_indices.get(name) {
            return index;
        }

        let position = self
            .kr
            .groups
            .partition_point(|group| group.name.as_slice() < name);
        self.kr.groups.insert(
            position,
            Group {
                name: name.to_vec(),
                files: Vec::new(),
                cached_sha: None,
            },
        );
        for index in self.kr.group_indices.values_mut() {
            if *index >= position {
                *index += 1;
            }
        }
        self.kr.group_indices.insert(name.to_vec(), position);
        self.kr.structure_dirty = true;
        position
    }

    /// Materializes and returns the current root tree object id.
    fn root_tree_sha(&mut self) -> Result<[u8; 20]> {
        // NOTE: 60% of commit_file() runtime

        //
        // Refresh only the dirty subtree in the steady state. Full group scans are only needed
        // when the `kr/` tree layout itself changed, such as the first time a new group appears.
        //
        if !self.tree_dirty
            && let Some(sha) = self.root.current_sha
        {
            return Ok(sha);
        }

        if self.kr.structure_dirty {
            for group in &mut self.kr.groups {
                if group.cached_sha.is_some() {
                    continue;
                }
                //
                // Group subtrees are only needed here, right before their SHA is refreshed. Keep
                // the serialization local so the byte layout is visible at the call site where it
                // matters: `100644/40000`, filename, NUL, then the child object id.
                //
                let tree = {
                    let mut tree = Vec::new();
                    for entry in &group.files {
                        tree.extend_from_slice(if entry.is_tree { b"40000 " } else { b"100644 " });
                        tree.extend_from_slice(&entry.name);
                        tree.push(0);
                        tree.extend_from_slice(&entry.sha);
                    }
                    tree
                };
                let sha = self.writer.write_object(PackObjectKind::Tree, &tree)?;
                group.cached_sha = Some(sha);
            }
        } else if let Some(index) = self.kr.dirty_group_index
            && self.kr.groups[index].cached_sha.is_none()
        {
            let group = &mut self.kr.groups[index];
            //
            // Most commits only touch one law file, so recomputing the affected subtree alone
            // avoids scanning every cached group on the hot path.
            //
            let tree = {
                let mut tree = Vec::new();
                for entry in &group.files {
                    tree.extend_from_slice(if entry.is_tree { b"40000 " } else { b"100644 " });
                    tree.extend_from_slice(&entry.name);
                    tree.push(0);
                    tree.extend_from_slice(&entry.sha);
                }
                tree
            };
            let sha = self.writer.write_object(PackObjectKind::Tree, &tree)?;
            group.cached_sha = Some(sha);
        }

        //
        // Rebuild or patch the cached kr/ tree, then remember its current object SHA.
        //
        let kr_tree = if self.kr.groups.is_empty() {
            self.kr.cache.clear();
            self.kr.sha_offsets.clear();
            self.kr.current_sha = None;
            self.kr.structure_dirty = false;
            self.kr.dirty_group_index = None;
            None
        } else {
            if self.kr.structure_dirty || self.kr.sha_offsets.len() != self.kr.groups.len() {
                self.kr.cache.clear();
                self.kr.sha_offsets.clear();
                for group in &self.kr.groups {
                    self.kr.cache.extend_from_slice(b"40000 ");
                    self.kr.cache.extend_from_slice(&group.name);
                    self.kr.cache.push(0);
                    self.kr.sha_offsets.push(self.kr.cache.len());
                    self.kr.cache.extend_from_slice(
                        &group.cached_sha.context("missing cached subtree SHA")?,
                    );
                }
                self.kr.structure_dirty = false;
                let kr_tree_sha = self
                    .writer
                    .write_object(PackObjectKind::Tree, &self.kr.cache)?;
                self.kr.current_sha = Some(kr_tree_sha);
                self.kr.dirty_group_index = None;
                Some(kr_tree_sha)
            } else if let Some(index) = self.kr.dirty_group_index.take() {
                let base_kr_tree_sha = self.kr.current_sha;
                let sha_offset = self.kr.sha_offsets[index];
                let new_group_sha = self.kr.groups[index]
                    .cached_sha
                    .context("missing cached subtree SHA")?;
                let delta = make_copy_insert_delta(self.kr.cache.len(), sha_offset, &new_group_sha);
                self.kr.cache[sha_offset..sha_offset + 20].copy_from_slice(&new_group_sha);
                let kr_tree_sha = git_hash(PackObjectKind::Tree.git_type_name(), &self.kr.cache);
                if let Some(base_kr_tree_sha) = base_kr_tree_sha {
                    self.writer
                        .write_ref_delta(base_kr_tree_sha, &delta, kr_tree_sha)?;
                } else {
                    self.writer
                        .write_object(PackObjectKind::Tree, &self.kr.cache)?;
                }
                self.kr.current_sha = Some(kr_tree_sha);
                Some(kr_tree_sha)
            } else if let Some(kr_tree_sha) = self.kr.current_sha {
                Some(kr_tree_sha)
            } else {
                let kr_tree_sha = self
                    .writer
                    .write_object(PackObjectKind::Tree, &self.kr.cache)?;
                self.kr.current_sha = Some(kr_tree_sha);
                Some(kr_tree_sha)
            }
        };

        //
        // Rebuild or patch the cached root tree bytes in the same way.
        //
        let root_structure_dirty = self.root.sha_offsets.len() != self.root.files.len()
            || self.root.kr_sha_offset.is_some() != kr_tree.is_some();
        let root_sha = if root_structure_dirty {
            self.root.cache.clear();
            self.root.sha_offsets.resize(self.root.files.len(), 0);
            self.root.kr_sha_offset = None;

            let mut root_entries =
                Vec::with_capacity(self.root.files.len() + usize::from(kr_tree.is_some()));
            for (index, file) in self.root.files.iter().enumerate() {
                root_entries.push((Some(index), &file.name[..], file.sha, false));
            }
            if let Some(kr_tree) = kr_tree {
                root_entries.push((None, b"kr".as_slice(), kr_tree, true));
            }
            //
            // Root-tree entries must follow Git's special tree ordering where directories sort
            // as though their names ended with `/`, not with a NUL terminator like files.
            //
            root_entries.sort_by(|left, right| {
                let common = left.1.len().min(right.1.len());
                match left.1[..common].cmp(&right.1[..common]) {
                    std::cmp::Ordering::Equal => {
                        let left_tail = if left.3 { b'/' } else { 0 };
                        let right_tail = if right.3 { b'/' } else { 0 };
                        let left_next = left.1.get(common).copied().unwrap_or(left_tail);
                        let right_next = right.1.get(common).copied().unwrap_or(right_tail);
                        left_next.cmp(&right_next)
                    }
                    other => other,
                }
            });

            for (kind, name, sha, is_tree) in root_entries {
                self.root
                    .cache
                    .extend_from_slice(if is_tree { b"40000 " } else { b"100644 " });
                self.root.cache.extend_from_slice(name);
                self.root.cache.push(0);
                let sha_offset = self.root.cache.len();
                if let Some(index) = kind {
                    self.root.sha_offsets[index] = sha_offset;
                } else {
                    self.root.kr_sha_offset = Some(sha_offset);
                }
                self.root.cache.extend_from_slice(&sha);
            }

            self.root.dirty_entry = None;
            self.writer
                .write_object(PackObjectKind::Tree, &self.root.cache)?
        } else if let Some(dirty) = self.root.dirty_entry.take() {
            let (sha_offset, new_sha) = match dirty {
                DirtyRootEntry::File(index) => {
                    let offset = self.root.sha_offsets[index];
                    (offset, self.root.files[index].sha)
                }
                DirtyRootEntry::Kr => {
                    let offset = self
                        .root
                        .kr_sha_offset
                        .context("missing cached root kr offset")?;
                    let sha = kr_tree.context("missing cached kr tree SHA")?;
                    (offset, sha)
                }
            };
            let delta = make_copy_insert_delta(self.root.cache.len(), sha_offset, &new_sha);
            self.root.cache[sha_offset..sha_offset + 20].copy_from_slice(&new_sha);
            let root_sha = git_hash(PackObjectKind::Tree.git_type_name(), &self.root.cache);
            if let Some(base_root_sha) = self.root.current_sha {
                self.writer
                    .write_ref_delta(base_root_sha, &delta, root_sha)?;
            } else {
                self.writer
                    .write_object(PackObjectKind::Tree, &self.root.cache)?;
            }
            root_sha
        } else if let Some(root_sha) = self.root.current_sha {
            root_sha
        } else {
            self.writer
                .write_object(PackObjectKind::Tree, &self.root.cache)?
        };

        self.root.current_sha = Some(root_sha);
        self.tree_dirty = false;
        Ok(root_sha)
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
        // NOTE: 5.0% of commit_file() runtime

        // Commit objects stay full-text because they are tiny and must exactly match Git's format.
        let mut commit = String::with_capacity(1000);
        commit.push_str(&format!("tree {}\n", hex(&tree)));
        if let Some(parent) = self.parent_commit {
            commit.push_str(&format!("parent {}\n", hex(&parent)));
        }
        commit.push_str(&format!(
            "author {} <{}> {} +0900\ncommitter {} <{}> {} +0900\n\n{message}",
            author.name, author.email, time.epoch, committer.name, committer.email, time.epoch
        ));
        self.writer
            .write_object(PackObjectKind::Commit, commit.as_bytes())
    }
}

impl PackWriter {
    /// Creates a new pack writer that writes directly to the final `.pack` file.
    fn new(path: &Path) -> Result<Self> {
        let mut file = BufWriter::with_capacity(4 << 20, File::create(path)?);
        // Write PACK header with placeholder object count (patched in finish()).
        let pack_header: [u8; 12] = [
            b'P', b'A', b'C', b'K', // magic
            0, 0, 0, 2, // version 2
            0, 0, 0, 0, // object count placeholder
        ];
        file.write_all(&pack_header)?;
        Ok(Self {
            file,
            object_count: 0,
            path: path.to_path_buf(),
            seen: HashSet::default(),
            idx_entries: Vec::new(),
            bytes_written: 12,
        })
    }

    /// Appends one full object to the pack unless it was already emitted.
    fn write_object(&mut self, object_type: PackObjectKind, data: &[u8]) -> Result<[u8; 20]> {
        //
        // Hash first so repeated trees/blobs/commits can be skipped entirely in the pack stream.
        //
        let sha = git_hash(object_type.git_type_name(), data);
        self.write_precompressed_object(object_type, data.len(), sha, &compress(data))
    }

    /// Appends one `REF_DELTA` object to the pack unless the result id already exists.
    fn write_ref_delta(
        &mut self,
        base_sha: [u8; 20],
        delta: &[u8],
        result_sha: [u8; 20],
    ) -> Result<[u8; 20]> {
        if !self.seen.insert(result_sha) {
            return Ok(result_sha);
        }

        let offset = self.bytes_written;
        let header_bytes = encode_pack_entry_header(PackObjectKind::RefDelta, delta.len());
        let compressed = compress(delta);

        // CRC-32 covers the raw pack entry bytes: header + base SHA + compressed delta.
        let mut crc = Crc::new();
        crc.update(&header_bytes);
        crc.update(&base_sha);
        crc.update(&compressed);

        self.file.write_all(&header_bytes)?;
        self.file.write_all(&base_sha)?;
        self.file.write_all(&compressed)?;
        self.bytes_written += header_bytes.len() as u64 + 20 + compressed.len() as u64;
        self.object_count += 1;
        self.idx_entries.push(IdxEntry {
            sha: result_sha,
            crc32: crc.sum(),
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
        if !self.seen.insert(sha) {
            return Ok(sha);
        }

        let offset = self.bytes_written;
        let header_bytes = encode_pack_entry_header(object_type, size);

        // CRC-32 covers the raw pack entry bytes: header + compressed payload.
        let mut crc = Crc::new();
        crc.update(&header_bytes);
        crc.update(compressed);

        self.file.write_all(&header_bytes)?;
        self.file.write_all(compressed)?;
        self.bytes_written += header_bytes.len() as u64 + compressed.len() as u64;
        self.object_count += 1;
        self.idx_entries.push(IdxEntry {
            sha,
            crc32: crc.sum(),
            offset,
        });
        Ok(sha)
    }

    /// Finalizes the pack file (patches object count, appends checksum) and generates the `.idx`.
    fn finish(&mut self) -> Result<()> {
        self.file.flush()?;

        // Patch the real object count at offset 8.
        let inner = self.file.get_mut();
        inner.seek(SeekFrom::Start(8))?;
        inner.write_all(&self.object_count.to_be_bytes())?;
        inner.flush()?;

        // Re-read entire file through SHA-1 hasher (streaming, 1MB chunks).
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

        // Append the 20-byte SHA-1 checksum to the pack file.
        let mut pack_file = fs::OpenOptions::new().append(true).open(&self.path)?;
        pack_file.write_all(&pack_checksum)?;
        pack_file.flush()?;
        drop(pack_file);

        // Generate .idx v2 file.
        self.write_idx_v2(&pack_checksum)?;

        // Rename tmp_pack.pack -> pack-{checksum_hex}.pack (and .idx similarly).
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
        // Sort index entries by SHA.
        self.idx_entries.sort_unstable_by(|a, b| a.sha.cmp(&b.sha));

        let idx_path = self.path.with_extension("idx");
        let mut f = BufWriter::with_capacity(4 << 20, File::create(&idx_path)?);
        let mut hasher = Sha1::new();

        // Helper: write bytes to both file and hasher.
        let mut write = |data: &[u8]| -> Result<()> {
            f.write_all(data)?;
            hasher.update(data);
            Ok(())
        };

        // Magic + version.
        write(&[0xff, 0x74, 0x4f, 0x63])?;
        write(&[0x00, 0x00, 0x00, 0x02])?;

        // Fanout table: 256 entries, each a cumulative count of objects whose first SHA byte <= i.
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

        // SHA table: n * 20 bytes, sorted.
        for entry in &self.idx_entries {
            write(&entry.sha)?;
        }

        // CRC32 table: n * 4 bytes BE.
        for entry in &self.idx_entries {
            write(&entry.crc32.to_be_bytes())?;
        }

        // Offset table: n * 4 bytes BE. Set bit 31 for offsets >= 2GB.
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

        // Optional 64-bit offset table for large offsets.
        for &off in &large_offsets {
            write(&off.to_be_bytes())?;
        }

        // Pack checksum.
        write(pack_checksum)?;

        // Flush the hasher through the closure, then compute idx checksum.
        f.flush()?;
        let idx_checksum: [u8; 20] = hasher.finalize().into();
        f.write_all(&idx_checksum)?;
        f.flush()?;

        Ok(())
    }
}

/// Encodes the variable-length PACK entry header into a stack buffer and returns it.
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

/// Creates a Git command with user config disabled for deterministic behavior.
#[cfg(test)]
fn git_command() -> Command {
    let mut command = Command::new("git");
    command.env("GIT_CONFIG_GLOBAL", "/dev/null");
    command.env("GIT_CONFIG_NOSYSTEM", "1");
    command.env_remove("GIT_DIR");
    command.env_remove("GIT_WORK_TREE");
    command
}

/// Converts a failed Git subprocess result into a rich error.
#[cfg(test)]
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

/// Inserts a sorted tree entry if needed and returns its index plus insertion status.
fn upsert(entries: &mut Vec<Entry>, name: &[u8], is_tree: bool) -> (usize, bool) {
    match entries.iter().position(|entry| entry.name == name) {
        Some(index) => (index, false),
        None => {
            let index = entries.partition_point(|entry| entry.name.as_slice() < name);
            entries.insert(
                index,
                Entry {
                    name: name.to_vec(),
                    sha: [0; 20],
                    is_tree,
                    previous_blob: None,
                },
            );
            (index, true)
        }
    }
}

thread_local! {
    /// Reuses one fast zlib compressor per thread for whole-buffer pack payload compression.
    static COMPRESSOR: RefCell<Compressor> =
        RefCell::new(Compressor::new(CompressionLvl::new(1).unwrap()));
}

/// Compresses one pack payload with the current fast zlib setting.
fn compress(data: &[u8]) -> Vec<u8> {
    COMPRESSOR.with(|compressor| {
        let mut compressor = compressor.borrow_mut();
        let mut output = vec![0; compressor.zlib_compress_bound(data.len())];
        let compressed = compressor
            .zlib_compress(data, &mut output)
            .expect("zlib_compress_bound() must allocate enough space");
        output.truncate(compressed);
        output
    })
}

/// Fixed block width used by the blob delta matcher.
const DELTA_BLOCK_SIZE: usize = 16;

/// Builds a Git copy/insert delta from `src` to `dst`.
#[inline(never)]
fn create_delta(src: &[u8], dst: &[u8]) -> Vec<u8> {
    // NOTE: 26% of commit_file() runtime

    //
    // Index fixed-size source blocks so destination scanning can prefer copy commands.
    //
    let mut delta = Vec::with_capacity(dst.len() / 2);
    encode_varint(&mut delta, src.len());
    encode_varint(&mut delta, dst.len());

    if src.len() < DELTA_BLOCK_SIZE {
        emit_inserts(&mut delta, dst);
        return delta;
    }

    let (source_blocks, _) = src.as_chunks();
    let source_block_count = source_blocks.len();
    let mut index: HashMap<u32, SmallVec<[usize; 4]>> =
        HashMap::with_capacity_and_hasher(source_block_count, Default::default());
    for (block_index, block) in source_blocks.iter().enumerate() {
        index
            .entry(block_hash(block))
            .or_default()
            .push(block_index * DELTA_BLOCK_SIZE);
    }

    //
    // Walk the destination once, alternating between copy commands and literal inserts.
    //
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

/// Builds a tiny copy/insert delta that swaps a single 20-byte SHA inside a cached tree.
fn make_copy_insert_delta(total: usize, offset: usize, new_sha: &[u8; 20]) -> Vec<u8> {
    // Tree delta updates only swap one 20-byte SHA, so they can be expressed as copy/insert/copy.
    let mut delta = Vec::with_capacity(64);
    encode_varint(&mut delta, total);
    encode_varint(&mut delta, total);

    if offset > 0 {
        emit_copy(&mut delta, 0, offset);
    }

    delta.push(20);
    delta.extend_from_slice(new_sha);

    let tail_offset = offset + 20;
    let tail_size = total - tail_offset;
    if tail_size > 0 {
        emit_copy(&mut delta, tail_offset, tail_size);
    }

    delta
}

/// Emits one Git delta copy instruction.
fn emit_copy(out: &mut Vec<u8>, offset: usize, size: usize) {
    // PACK copy opcodes only encode the non-zero bytes of the offset and size fields.
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

/// Hashes one fixed-size block for delta index lookup.
fn block_hash(data: &[u8; DELTA_BLOCK_SIZE]) -> u32 {
    // This hash only drives candidate bucketing inside create_delta(). Collisions are harmless,
    // so prefer a cheap fixed-width mix over a stronger byte-at-a-time hash.
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

        // Short-circuit
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

/// Computes the canonical Git object id for one unhashed object body.
fn git_hash(type_name: &[u8], data: &[u8]) -> [u8; 20] {
    let mut hasher = Sha1::new();
    // Avoid allocating a header string in this hot hash path.
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

/// Hex-encodes one object id for commit bodies and Git subprocess arguments.
fn hex(sha: &[u8; 20]) -> String {
    let mut encoded = String::with_capacity(40);
    for byte in sha {
        use std::fmt::Write as _;
        write!(encoded, "{byte:02x}").expect("formatting into String cannot fail");
    }
    encoded
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use tempfile::TempDir;

    use super::*;

    fn output_repo(temp: &TempDir) -> PathBuf {
        temp.path().join("output.git")
    }

    fn new_writer(temp: &TempDir) -> (PathBuf, BareRepoWriter) {
        let output = output_repo(temp);
        let writer = BareRepoWriter::create(&output).unwrap();
        (output, writer)
    }

    #[test]
    fn clamps_pre_epoch_dates() {
        let temp = TempDir::new().unwrap();
        let (output, mut writer) = new_writer(&temp);
        let (blob_sha, compressed_blob) = precompute_blob(b"body");
        writer
            .commit_law(
                &RepoPathBuf::kr_file("테스트법", "법률.md"),
                b"body",
                blob_sha,
                &compressed_blob,
                "message",
                GitTimestampKst::from_promulgation_date("19491021").unwrap(),
            )
            .unwrap();
        writer.finish().unwrap();

        let epoch = git_stdout(&output, ["show", "-s", "--format=%at", "HEAD"]);
        let date = git_stdout(&output, ["show", "-s", "--format=%ai", "HEAD"]);
        assert_eq!(epoch.trim(), "10800");
        assert_eq!(date.trim(), "1970-01-01 12:00:00 +0900");
    }

    #[test]
    fn rejects_non_compact_promulgation_dates() {
        let error = GitTimestampKst::from_promulgation_date("2024-01-01").unwrap_err();
        assert!(error.to_string().contains("YYYYMMDD"));
    }

    #[test]
    fn blob_delta_is_smaller_for_similar_versions() {
        let before = b"# test\n\nalpha\nbeta\ngamma\n";
        let after = b"# test\n\nalpha\nbeta\ngamma\ndelta\n";
        let delta = create_delta(before, after);
        assert!(delta.len() < after.len());
    }

    #[test]
    fn finish_supports_relative_output_paths() {
        let temp = TempDir::new().unwrap();
        let previous_dir = std::env::current_dir().unwrap();
        std::env::set_current_dir(temp.path()).unwrap();

        let result = (|| -> Result<()> {
            let mut writer = BareRepoWriter::create(Path::new("output.git"))?;
            let (blob_sha, compressed_blob) = precompute_blob(b"body");
            writer.commit_law(
                &RepoPathBuf::kr_file("테스트법", "법률.md"),
                b"body",
                blob_sha,
                &compressed_blob,
                "message",
                GitTimestampKst::from_promulgation_date("20240101")?,
            )?;
            writer.finish()?;
            Ok(())
        })();

        std::env::set_current_dir(previous_dir).unwrap();
        result.unwrap();

        let output = output_repo(&temp);
        assert_eq!(
            git_stdout(&output, ["rev-list", "--count", "HEAD"]).trim(),
            "1"
        );
    }

    #[test]
    fn finished_repo_is_cloneable_without_git_init_scaffolding() {
        let temp = TempDir::new().unwrap();
        let clone = temp.path().join("clone");
        let (output, mut writer) = new_writer(&temp);
        writer
            .commit_static(
                &RepoPathBuf::root_file("README.md"),
                b"hello\n",
                "initial commit",
                1_774_839_600,
            )
            .unwrap();
        writer.finish().unwrap();

        assert!(!output.join("config").exists());
        assert!(!output.join("description").exists());
        assert_eq!(
            git_stdout(&output, ["rev-list", "--count", "HEAD"]).trim(),
            "1"
        );

        let clone_output = git_command()
            .arg("clone")
            .arg(&output)
            .arg(&clone)
            .output()
            .unwrap();
        ensure_command_success(clone_output, "git clone").unwrap();
        assert_eq!(
            fs::read_to_string(clone.join("README.md")).unwrap(),
            "hello\n"
        );
    }

    #[test]
    fn root_tree_delta_handles_root_file_updates() {
        let temp = TempDir::new().unwrap();
        let (output, mut writer) = new_writer(&temp);
        writer
            .commit_static(
                &RepoPathBuf::root_file("README.md"),
                b"first\n",
                "initial commit",
                1_774_839_600,
            )
            .unwrap();
        writer
            .commit_static(
                &RepoPathBuf::root_file("README.md"),
                b"second\n",
                "update readme",
                1_774_839_601,
            )
            .unwrap();
        writer.finish().unwrap();

        assert_eq!(
            git_stdout(&output, ["rev-list", "--count", "HEAD"]).trim(),
            "2"
        );
        assert_eq!(git_stdout(&output, ["show", "HEAD:README.md"]), "second\n");
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
