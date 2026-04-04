use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{self, Command, Output};

use anyhow::{Context, Result, anyhow, bail};
use flate2::Compression;
use flate2::write::ZlibEncoder;
use openssl::sha;
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
        // Normalize both `YYYYMMDD` and `YYYY-MM-DD` forms into the canonical date string the
        // historical pipeline implicitly used when deriving commit timestamps.
        //
        let effective_date = if promulgation_date.len() == 8
            && promulgation_date.bytes().all(|byte| byte.is_ascii_digit())
        {
            format!(
                "{}-{}-{}",
                &promulgation_date[..4],
                &promulgation_date[4..6],
                &promulgation_date[6..8]
            )
        } else {
            promulgation_date.to_owned()
        };

        //
        // Clamp malformed inputs and pre-epoch dates before conversion so reruns keep producing the
        // same commit ids even when upstream metadata is incomplete or predates Unix time.
        //
        let effective_date = if effective_date.len() != 10 {
            String::from("2000-01-01")
        } else if effective_date.as_str() < "1970-01-01" {
            String::from("1970-01-01")
        } else {
            effective_date
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

/// Low-level writer that streams raw packfile entries before final assembly.
struct PackWriter {
    // Object payloads are buffered separately so finish() can stream a final pack
    // header with the real object count instead of patching bytes in place.
    /// Temporary body stream containing pack entries without the final header.
    body_file: BufWriter<File>,
    /// Filesystem path of the temporary pack body stream.
    body_path: PathBuf,
    /// Number of unique objects appended to the pack body.
    object_count: u32,
    /// Final `.pack` destination path inside the temporary bare repo.
    path: PathBuf,
    /// Object ids already emitted into the pack stream.
    seen: HashSet<[u8; 20]>,
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

    // Blob and subtree history that lets repeated law revisions reuse previous objects.
    /// Previous blob bodies keyed by repository path.
    prev_blobs: HashMap<RepoPathBuf, PreviousBlob>,
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

        init_bare_repo(&temp_output)?;

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
            prev_blobs: HashMap::new(),
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
        message: &str,
        time: GitTimestampKst,
    ) -> Result<()> {
        let bot = GitPerson {
            name: "legalize-kr-bot",
            email: "bot@legalize.kr",
        };
        self.commit_file(path, markdown, message, bot, bot, time)
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
        self.commit_file(
            path,
            content,
            &message,
            author,
            author,
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

        let pack_path = Path::new("objects/pack/tmp_pack.pack");
        let output = git_command()
            .arg("-C")
            .arg(&self.temp_output)
            .arg("index-pack")
            .arg(pack_path)
            .output()
            .context("failed to run git index-pack")?;
        ensure_command_success(output, "git index-pack")?;

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
        message: &str,
        author: GitPerson<'_>,
        committer: GitPerson<'_>,
        time: GitTimestampKst,
    ) -> Result<()> {
        //
        // Store the file body first, preferably as a delta against the previous revision.
        //
        let blob_sha = git_hash(PackObjectKind::Blob.git_type_name(), content);
        if let Some(previous) = self.prev_blobs.get(path) {
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
            if previous.sha != blob_sha && smaller >= 128 && larger <= smaller.saturating_mul(2) {
                let delta = create_delta(&previous.content, content);
                if delta.len() < content.len() * 3 / 4 {
                    self.writer
                        .write_ref_delta(previous.sha, &delta, blob_sha)?;
                } else {
                    self.writer.write_object(PackObjectKind::Blob, content)?;
                }
            } else {
                self.writer.write_object(PackObjectKind::Blob, content)?;
            }
        } else {
            self.writer.write_object(PackObjectKind::Blob, content)?;
        }
        self.prev_blobs.insert(
            path.clone(),
            PreviousBlob {
                sha: blob_sha,
                content: content.to_vec(),
            },
        );

        //
        // Update the logical tree state for either a root file or a kr/<group>/<file> leaf.
        //
        match path {
            RepoPathBuf::RootFile(name) => {
                let (index, inserted) =
                    upsert(&mut self.root.files, name.as_bytes(), blob_sha, false);
                if inserted {
                    self.root.sha_offsets.clear();
                    self.root.kr_sha_offset = None;
                    self.root.dirty_entry = None;
                } else {
                    self.root.dirty_entry = Some(DirtyRootEntry::File(index));
                }
                self.kr.dirty_group_index = None;
            }
            RepoPathBuf::KrFile { group, filename } => {
                let group_index = self.ensure_group(group.as_bytes());
                upsert(
                    &mut self.kr.groups[group_index].files,
                    filename.as_bytes(),
                    blob_sha,
                    false,
                );
                self.kr.groups[group_index].cached_sha = None;
                self.kr.dirty_group_index = Some(group_index);
                self.root.dirty_entry = Some(DirtyRootEntry::Kr);
            }
        }

        //
        // Materialize the current root tree and append the commit object in order.
        //
        self.tree_dirty = true;
        let root_sha = self.root_tree_sha()?;
        let commit_sha = self.write_commit(root_sha, message, author, committer, time)?;
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
        //
        // Refresh per-group subtree SHAs only for groups whose file set changed.
        //
        if !self.tree_dirty
            && let Some(sha) = self.root.current_sha
        {
            return Ok(sha);
        }

        for group in &mut self.kr.groups {
            if group.cached_sha.is_some() {
                continue;
            }
            //
            // Group subtrees are only needed here, right before their SHA is refreshed. Keep the
            // serialization local so the byte layout is visible at the call site where it matters:
            // `100644/40000`, filename, NUL, then the child object id.
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
        // Commit objects stay full-text because they are tiny and must exactly match Git's format.
        let mut commit = format!("tree {}\n", hex(&tree));
        if let Some(parent) = self.parent_commit {
            commit.push_str(&format!("parent {}\n", hex(&parent)));
        }
        commit.push_str(&format!(
            "author {} <{}> {} +0900\n",
            author.name, author.email, time.epoch
        ));
        commit.push_str(&format!(
            "committer {} <{}> {} +0900\n",
            committer.name, committer.email, time.epoch
        ));
        commit.push('\n');
        commit.push_str(message);
        self.writer
            .write_object(PackObjectKind::Commit, commit.as_bytes())
    }
}

impl PackWriter {
    /// Creates a new pack writer that buffers entry bodies in a temporary file.
    fn new(path: &Path) -> Result<Self> {
        let body_path = path.with_extension("pack.body");
        let body_file = BufWriter::with_capacity(1 << 20, File::create(&body_path)?);
        Ok(Self {
            body_file,
            body_path,
            object_count: 0,
            path: path.to_path_buf(),
            seen: HashSet::new(),
        })
    }

    /// Appends one full object to the pack unless it was already emitted.
    fn write_object(&mut self, object_type: PackObjectKind, data: &[u8]) -> Result<[u8; 20]> {
        //
        // Hash first so repeated trees/blobs/commits can be skipped entirely in the pack stream.
        //
        let sha = git_hash(object_type.git_type_name(), data);
        if !self.seen.insert(sha) {
            return Ok(sha);
        }

        //
        // PACK object headers use a variable-length size encoding ahead of the compressed body.
        //
        self.write_pack_entry_header(object_type, data.len())?;
        self.write_raw(&compress(data))?;
        self.object_count += 1;
        Ok(sha)
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

        // REF_DELTA stores the base object id before the compressed delta payload.
        self.write_pack_entry_header(PackObjectKind::RefDelta, delta.len())?;
        self.write_raw(&base_sha)?;
        self.write_raw(&compress(delta))?;
        self.object_count += 1;
        Ok(result_sha)
    }

    /// Writes the final pack header and trailer checksum around the buffered body stream.
    fn finish(&mut self) -> Result<()> {
        //
        // Assemble the final pack in one streamed pass so finish() does not reread the whole file.
        //
        self.body_file.flush()?;

        let mut output = BufWriter::with_capacity(1 << 20, File::create(&self.path)?);
        let mut hasher = sha::Sha1::new();

        let pack_header = [
            b'P',
            b'A',
            b'C',
            b'K',
            0,
            0,
            0,
            2,
            (self.object_count >> 24) as u8,
            (self.object_count >> 16) as u8,
            (self.object_count >> 8) as u8,
            self.object_count as u8,
        ];
        output.write_all(&pack_header)?;
        hasher.update(&pack_header);

        let mut body = BufReader::with_capacity(1 << 20, File::open(&self.body_path)?);
        let mut buffer = [0_u8; 1 << 20];
        loop {
            let read = body.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            let chunk = &buffer[..read];
            output.write_all(chunk)?;
            hasher.update(chunk);
        }

        output.write_all(&hasher.finish())?;
        output.flush()?;
        fs::remove_file(&self.body_path)
            .with_context(|| format!("failed to remove {}", self.body_path.display()))?;
        Ok(())
    }

    /// Writes raw bytes into the temporary pack body stream.
    fn write_raw(&mut self, bytes: &[u8]) -> Result<()> {
        self.body_file.write_all(bytes)?;
        Ok(())
    }

    #[inline]
    /// Encodes the variable-length PACK entry header for one object payload.
    fn write_pack_entry_header(&mut self, object_type: PackObjectKind, size: usize) -> Result<()> {
        let mut header = ((object_type as u8 & 0b111) << 4) | (size as u8 & 0x0f);
        let mut remaining = size >> 4;
        if remaining > 0 {
            header |= 0x80;
        }
        self.write_raw(&[header])?;
        while remaining > 0 {
            let mut byte = (remaining & 0x7f) as u8;
            remaining >>= 7;
            if remaining > 0 {
                byte |= 0x80;
            }
            self.write_raw(&[byte])?;
        }
        Ok(())
    }
}

/// Initializes the temporary bare repository with the standard files ref backend.
fn init_bare_repo(repo_dir: &Path) -> Result<()> {
    const MAIN_BRANCH: &str = "main";

    //
    // This compiler only writes `HEAD` and `refs/heads/main`, so loose ref files are simpler than
    // carrying reftable compatibility logic or an extra `git update-ref` subprocess.
    //
    let output = git_command()
        .arg("init")
        .arg("--quiet")
        .arg("--bare")
        .arg("--initial-branch")
        .arg(MAIN_BRANCH)
        .arg(repo_dir)
        .output()
        .with_context(|| format!("failed to init bare repo at {}", repo_dir.display()))?;
    ensure_command_success(output, "git init --bare")
}

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

/// Inserts or updates one sorted tree entry and returns its index plus insertion status.
fn upsert(entries: &mut Vec<Entry>, name: &[u8], sha: [u8; 20], is_tree: bool) -> (usize, bool) {
    match entries.iter().position(|entry| entry.name == name) {
        Some(index) => {
            entries[index].sha = sha;
            (index, false)
        }
        None => {
            let index = entries.partition_point(|entry| entry.name.as_slice() < name);
            entries.insert(
                index,
                Entry {
                    name: name.to_vec(),
                    sha,
                    is_tree,
                },
            );
            (index, true)
        }
    }
}

/// Compresses one pack payload with the current fast zlib setting.
fn compress(data: &[u8]) -> Vec<u8> {
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::fast());
    encoder
        .write_all(data)
        .expect("zlib write to Vec cannot fail");
    encoder.finish().expect("zlib finish on Vec cannot fail")
}

/// Builds a Git copy/insert delta from `src` to `dst`.
fn create_delta(src: &[u8], dst: &[u8]) -> Vec<u8> {
    let block_size = 16usize;

    //
    // Index fixed-size source blocks so destination scanning can prefer copy commands.
    //
    let mut delta = Vec::with_capacity(dst.len() / 2);
    encode_varint(&mut delta, src.len());
    encode_varint(&mut delta, dst.len());

    if src.len() < block_size {
        emit_inserts(&mut delta, dst);
        return delta;
    }

    let mut index = HashMap::<u32, Vec<usize>>::new();
    for source_offset in (0..src.len().saturating_sub(block_size - 1)).step_by(block_size) {
        let hash = block_hash(&src[source_offset..source_offset + block_size]);
        index.entry(hash).or_default().push(source_offset);
    }

    //
    // Walk the destination once, alternating between copy commands and literal inserts.
    //
    let mut destination_offset = 0usize;
    let mut pending = Vec::new();

    while destination_offset < dst.len() {
        let mut best_source_offset = 0usize;
        let mut best_len = 0usize;
        let remaining = dst.len() - destination_offset;

        if remaining >= block_size {
            let hash = block_hash(&dst[destination_offset..destination_offset + block_size]);
            if let Some(candidates) = index.get(&hash) {
                for &source_offset in candidates {
                    let match_len = match_length(src, source_offset, dst, destination_offset);
                    if match_len > best_len {
                        best_len = match_len;
                        best_source_offset = source_offset;
                    }
                }
            }
        }

        if best_len >= block_size {
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
    let mut args = Vec::with_capacity(7);
    if offset & 0xff != 0 {
        command |= 0x01;
        args.push((offset & 0xff) as u8);
    }
    if offset & 0xff00 != 0 {
        command |= 0x02;
        args.push(((offset >> 8) & 0xff) as u8);
    }
    if offset & 0xff0000 != 0 {
        command |= 0x04;
        args.push(((offset >> 16) & 0xff) as u8);
    }
    if offset & 0xff000000 != 0 {
        command |= 0x08;
        args.push(((offset >> 24) & 0xff) as u8);
    }
    if size & 0xff != 0 {
        command |= 0x10;
        args.push((size & 0xff) as u8);
    }
    if size & 0xff00 != 0 {
        command |= 0x20;
        args.push(((size >> 8) & 0xff) as u8);
    }
    if size & 0xff0000 != 0 {
        command |= 0x40;
        args.push(((size >> 16) & 0xff) as u8);
    }
    out.push(command);
    out.extend_from_slice(&args);
}

/// Hashes one fixed-size block for delta index lookup.
fn block_hash(data: &[u8]) -> u32 {
    let mut hash = 0x811c9dc5u32;
    for &byte in data {
        hash ^= byte as u32;
        hash = hash.wrapping_mul(0x01000193);
    }
    hash
}

/// Returns the byte length of the common run starting at the two offsets.
fn match_length(src: &[u8], src_offset: usize, dst: &[u8], dst_offset: usize) -> usize {
    let max = std::cmp::min(src.len() - src_offset, dst.len() - dst_offset);
    let mut len = 0usize;
    while len < max && src[src_offset + len] == dst[dst_offset + len] {
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
    let mut hasher = sha::Sha1::new();
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
    hasher.update(&[0]);
    hasher.update(data);
    hasher.finish()
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
        writer
            .commit_law(
                &RepoPathBuf::kr_file("테스트법", "법률.md"),
                b"body",
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
            writer.commit_law(
                &RepoPathBuf::kr_file("테스트법", "법률.md"),
                b"body",
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
