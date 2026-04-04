use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{self, Command, Output};

use anyhow::{Context, Result, anyhow, bail};
use flate2::Compression;
use flate2::write::ZlibEncoder;
use openssl::sha;
use time::{Date, Month, PrimitiveDateTime, Time as CivilTime, UtcOffset};

const MAIN_BRANCH: &str = "main";
const MAIN_REF: &str = "refs/heads/main";
const BOT_NAME: &str = "legalize-kr-bot";
const BOT_EMAIL: &str = "bot@legalize.kr";
const INITIAL_COMMIT_AUTHOR_NAME: &str = "Junghwan Park";
const INITIAL_COMMIT_AUTHOR_EMAIL: &str = "reserve.dev@gmail.com";
const INITIAL_COMMIT_CO_AUTHORS: &[(&str, &str)] = &[("Jihyeon Kim", "simnalamburt@gmail.com")];
const INITIAL_COMMIT_COMMITTER_NAME: &str = "Jihyeon Kim";
const INITIAL_COMMIT_COMMITTER_EMAIL: &str = "simnalamburt@gmail.com";
const PACK_OBJECT_COMMIT: u8 = 1;
const PACK_OBJECT_TREE: u8 = 2;
const PACK_OBJECT_BLOB: u8 = 3;
const BLOCK_SIZE: usize = 16;
const INDEX_STEP: usize = 16;

#[derive(Debug, Clone, Copy)]
struct GitPerson<'a> {
    name: &'a str,
    email: &'a str,
}

#[derive(Debug, Clone, Copy)]
struct GitTimestamp {
    epoch: i64,
    offset_minutes: i32,
}

#[derive(Debug, Clone)]
struct Entry {
    name: Vec<u8>,
    sha: [u8; 20],
    is_tree: bool,
}

#[derive(Debug, Clone)]
struct Group {
    name: Vec<u8>,
    files: Vec<Entry>,
    cached_sha: Option<[u8; 20]>,
}

#[derive(Debug, Clone, Copy)]
enum DirtyRootEntry {
    File(usize),
    Kr,
}

struct PackWriter {
    // Object payloads are buffered separately so finish() can stream a final pack
    // header with the real object count instead of patching bytes in place.
    body_file: BufWriter<File>,
    body_path: PathBuf,
    object_count: u32,
    path: PathBuf,
    seen: HashSet<[u8; 20]>,
}

/// Writes the generated law history into a fresh bare Git repository.
pub struct BareRepoWriter {
    writer: PackWriter,
    temp_output: PathBuf,
    final_output: PathBuf,

    // Root-level files plus the cached serialized root tree used for REF_DELTA updates.
    root_files: Vec<Entry>,
    root_tree_cache: Vec<u8>,
    root_tree_sha_offsets: Vec<usize>,
    root_tree_kr_sha_offset: Option<usize>,
    dirty_root_entry: Option<DirtyRootEntry>,

    // Blob and subtree history that lets repeated law revisions reuse previous objects.
    prev_blobs: HashMap<String, ([u8; 20], Vec<u8>)>,
    groups: Vec<Group>,
    group_indices: HashMap<Vec<u8>, usize>,
    kr_tree_cache: Vec<u8>,
    kr_tree_sha_offsets: Vec<usize>,
    current_kr_tree_sha: Option<[u8; 20]>,
    kr_tree_structure_dirty: bool,
    dirty_group_index: Option<usize>,
    parent_commit: Option<[u8; 20]>,
    current_root_sha: Option<[u8; 20]>,
    tree_dirty: bool,
}

impl BareRepoWriter {
    /// Creates a new temporary bare repository writer for the requested output path.
    pub fn create(output: &Path) -> Result<Self> {
        let final_output = output.to_path_buf();
        let temp_output = make_temp_output_path(output)?;
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
            root_files: Vec::new(),
            root_tree_cache: Vec::new(),
            root_tree_sha_offsets: Vec::new(),
            root_tree_kr_sha_offset: None,
            dirty_root_entry: None,
            prev_blobs: HashMap::new(),
            groups: Vec::new(),
            group_indices: HashMap::new(),
            kr_tree_cache: Vec::new(),
            kr_tree_sha_offsets: Vec::new(),
            current_kr_tree_sha: None,
            kr_tree_structure_dirty: false,
            dirty_group_index: None,
            parent_commit: None,
            current_root_sha: None,
            tree_dirty: false,
        })
    }

    /// Commits one rendered law Markdown file using bot authorship and law dates.
    pub fn commit_law(
        &mut self,
        path: &str,
        markdown: &[u8],
        message: &str,
        promulgation_date: &str,
    ) -> Result<()> {
        let time = commit_time(promulgation_date)?;
        self.commit_file(
            path,
            markdown,
            message,
            GitPerson {
                name: BOT_NAME,
                email: BOT_EMAIL,
            },
            GitPerson {
                name: BOT_NAME,
                email: BOT_EMAIL,
            },
            time,
        )
    }

    /// Commits a static repository file with the fixed initial authorship metadata.
    pub fn commit_static(
        &mut self,
        path: &str,
        content: &[u8],
        message: &str,
        epoch: i64,
        offset_minutes: i32,
    ) -> Result<()> {
        let message = append_co_author_trailers(message, INITIAL_COMMIT_CO_AUTHORS);
        let author = GitPerson {
            name: INITIAL_COMMIT_AUTHOR_NAME,
            email: INITIAL_COMMIT_AUTHOR_EMAIL,
        };
        self.commit_file(
            path,
            content,
            &message,
            author,
            author,
            GitTimestamp {
                epoch,
                offset_minutes,
            },
        )
    }

    /// Appends the empty historical contributor commit after the initial static files.
    pub fn commit_empty_initial_contributor(
        &mut self,
        message: &str,
        epoch: i64,
        offset_minutes: i32,
    ) -> Result<()> {
        if self.parent_commit.is_none() {
            bail!("empty contributor commit requires an existing tree");
        }
        let author = GitPerson {
            name: INITIAL_COMMIT_COMMITTER_NAME,
            email: INITIAL_COMMIT_COMMITTER_EMAIL,
        };
        let root_sha = self.root_tree_sha()?;
        let commit_sha = self.write_commit(
            root_sha,
            message,
            author,
            author,
            GitTimestamp {
                epoch,
                offset_minutes,
            },
        )?;
        self.parent_commit = Some(commit_sha);
        Ok(())
    }

    /// Finalizes the pack, updates `main`, and moves the temporary repo into place.
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
            let output = git_command()
                .arg("-C")
                .arg(&self.temp_output)
                .arg("update-ref")
                .arg(MAIN_REF)
                .arg(hex(&parent_commit))
                .output()
                .context("failed to run git update-ref")?;
            ensure_command_success(output, "git update-ref")?;
        }

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

    fn commit_file(
        &mut self,
        path: &str,
        content: &[u8],
        message: &str,
        author: GitPerson<'_>,
        committer: GitPerson<'_>,
        time: GitTimestamp,
    ) -> Result<()> {
        //
        // Store the file body first, preferably as a delta against the previous revision.
        //
        ensure_repo_path(path)?;
        let blob_sha = git_hash(object_type_name(PACK_OBJECT_BLOB), content);
        if let Some((base_sha, base_content)) = self.prev_blobs.get(path) {
            let delta = create_delta(base_content, content);
            if delta.len() < content.len() * 3 / 4 {
                self.writer.write_ref_delta(*base_sha, &delta, blob_sha)?;
            } else {
                self.writer.write_object(PACK_OBJECT_BLOB, content)?;
            }
        } else {
            self.writer.write_object(PACK_OBJECT_BLOB, content)?;
        }
        self.prev_blobs
            .insert(path.to_owned(), (blob_sha, content.to_vec()));

        //
        // Update the logical tree state for either a root file or a kr/<group>/<file> leaf.
        //
        match path
            .split('/')
            .filter(|segment| !segment.is_empty())
            .collect::<Vec<_>>()
            .as_slice()
        {
            [name] => {
                let (index, inserted) =
                    upsert(&mut self.root_files, name.as_bytes(), blob_sha, false);
                if inserted {
                    self.root_tree_sha_offsets.clear();
                    self.root_tree_kr_sha_offset = None;
                    self.dirty_root_entry = None;
                } else {
                    self.dirty_root_entry = Some(DirtyRootEntry::File(index));
                }
                self.dirty_group_index = None;
            }
            ["kr", group, filename] => {
                let group_index = self.ensure_group(group.as_bytes());
                upsert(
                    &mut self.groups[group_index].files,
                    filename.as_bytes(),
                    blob_sha,
                    false,
                );
                self.groups[group_index].cached_sha = None;
                self.dirty_group_index = Some(group_index);
                self.dirty_root_entry = Some(DirtyRootEntry::Kr);
            }
            _ => bail!("unsupported repository path: {path}"),
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

    fn ensure_group(&mut self, name: &[u8]) -> usize {
        if let Some(&index) = self.group_indices.get(name) {
            return index;
        }

        let position = self
            .groups
            .partition_point(|group| group.name.as_slice() < name);
        self.groups.insert(
            position,
            Group {
                name: name.to_vec(),
                files: Vec::new(),
                cached_sha: None,
            },
        );
        for index in self.group_indices.values_mut() {
            if *index >= position {
                *index += 1;
            }
        }
        self.group_indices.insert(name.to_vec(), position);
        self.kr_tree_structure_dirty = true;
        position
    }

    fn root_tree_sha(&mut self) -> Result<[u8; 20]> {
        //
        // Refresh per-group subtree SHAs only for groups whose file set changed.
        //
        if !self.tree_dirty
            && let Some(sha) = self.current_root_sha
        {
            return Ok(sha);
        }

        for group in &mut self.groups {
            if group.cached_sha.is_some() {
                continue;
            }
            let tree = tree_bytes(&group.files);
            let sha = self.writer.write_object(PACK_OBJECT_TREE, &tree)?;
            group.cached_sha = Some(sha);
        }

        //
        // Rebuild or patch the cached kr/ tree, then remember its current object SHA.
        //
        let kr_tree = if self.groups.is_empty() {
            self.kr_tree_cache.clear();
            self.kr_tree_sha_offsets.clear();
            self.current_kr_tree_sha = None;
            self.kr_tree_structure_dirty = false;
            self.dirty_group_index = None;
            None
        } else {
            if self.kr_tree_structure_dirty || self.kr_tree_sha_offsets.len() != self.groups.len() {
                self.kr_tree_cache.clear();
                self.kr_tree_sha_offsets.clear();
                for group in &self.groups {
                    self.kr_tree_cache.extend_from_slice(b"40000 ");
                    self.kr_tree_cache.extend_from_slice(&group.name);
                    self.kr_tree_cache.push(0);
                    self.kr_tree_sha_offsets.push(self.kr_tree_cache.len());
                    self.kr_tree_cache.extend_from_slice(
                        &group.cached_sha.context("missing cached subtree SHA")?,
                    );
                }
                self.kr_tree_structure_dirty = false;
                let kr_tree_sha = self
                    .writer
                    .write_object(PACK_OBJECT_TREE, &self.kr_tree_cache)?;
                self.current_kr_tree_sha = Some(kr_tree_sha);
                self.dirty_group_index = None;
                Some(kr_tree_sha)
            } else if let Some(index) = self.dirty_group_index.take() {
                let base_kr_tree_sha = self.current_kr_tree_sha;
                let sha_offset = self.kr_tree_sha_offsets[index];
                let new_group_sha = self.groups[index]
                    .cached_sha
                    .context("missing cached subtree SHA")?;
                let delta =
                    make_copy_insert_delta(self.kr_tree_cache.len(), sha_offset, &new_group_sha);
                self.kr_tree_cache[sha_offset..sha_offset + 20].copy_from_slice(&new_group_sha);
                let kr_tree_sha = git_hash(object_type_name(PACK_OBJECT_TREE), &self.kr_tree_cache);
                if let Some(base_kr_tree_sha) = base_kr_tree_sha {
                    self.writer
                        .write_ref_delta(base_kr_tree_sha, &delta, kr_tree_sha)?;
                } else {
                    self.writer
                        .write_object(PACK_OBJECT_TREE, &self.kr_tree_cache)?;
                }
                self.current_kr_tree_sha = Some(kr_tree_sha);
                Some(kr_tree_sha)
            } else if let Some(kr_tree_sha) = self.current_kr_tree_sha {
                Some(kr_tree_sha)
            } else {
                let kr_tree_sha = self
                    .writer
                    .write_object(PACK_OBJECT_TREE, &self.kr_tree_cache)?;
                self.current_kr_tree_sha = Some(kr_tree_sha);
                Some(kr_tree_sha)
            }
        };

        //
        // Rebuild or patch the cached root tree bytes in the same way.
        //
        let root_structure_dirty = self.root_tree_sha_offsets.len() != self.root_files.len()
            || self.root_tree_kr_sha_offset.is_some() != kr_tree.is_some();
        let root_sha = if root_structure_dirty {
            self.root_tree_cache.clear();
            self.root_tree_sha_offsets.resize(self.root_files.len(), 0);
            self.root_tree_kr_sha_offset = None;

            let mut root_entries =
                Vec::with_capacity(self.root_files.len() + usize::from(kr_tree.is_some()));
            for (index, file) in self.root_files.iter().enumerate() {
                root_entries.push((Some(index), &file.name[..], file.sha, false));
            }
            if let Some(kr_tree) = kr_tree {
                root_entries.push((None, b"kr".as_slice(), kr_tree, true));
            }
            root_entries.sort_by(|left, right| {
                tree_sort_cmp(&(left.1, left.2, left.3), &(right.1, right.2, right.3))
            });

            for (kind, name, sha, is_tree) in root_entries {
                self.root_tree_cache.extend_from_slice(if is_tree {
                    b"40000 "
                } else {
                    b"100644 "
                });
                self.root_tree_cache.extend_from_slice(name);
                self.root_tree_cache.push(0);
                let sha_offset = self.root_tree_cache.len();
                if let Some(index) = kind {
                    self.root_tree_sha_offsets[index] = sha_offset;
                } else {
                    self.root_tree_kr_sha_offset = Some(sha_offset);
                }
                self.root_tree_cache.extend_from_slice(&sha);
            }

            self.dirty_root_entry = None;
            self.writer
                .write_object(PACK_OBJECT_TREE, &self.root_tree_cache)?
        } else if let Some(dirty) = self.dirty_root_entry.take() {
            let (sha_offset, new_sha) = match dirty {
                DirtyRootEntry::File(index) => {
                    let offset = self.root_tree_sha_offsets[index];
                    (offset, self.root_files[index].sha)
                }
                DirtyRootEntry::Kr => {
                    let offset = self
                        .root_tree_kr_sha_offset
                        .context("missing cached root kr offset")?;
                    let sha = kr_tree.context("missing cached kr tree SHA")?;
                    (offset, sha)
                }
            };
            let delta = make_copy_insert_delta(self.root_tree_cache.len(), sha_offset, &new_sha);
            self.root_tree_cache[sha_offset..sha_offset + 20].copy_from_slice(&new_sha);
            let root_sha = git_hash(object_type_name(PACK_OBJECT_TREE), &self.root_tree_cache);
            if let Some(base_root_sha) = self.current_root_sha {
                self.writer
                    .write_ref_delta(base_root_sha, &delta, root_sha)?;
            } else {
                self.writer
                    .write_object(PACK_OBJECT_TREE, &self.root_tree_cache)?;
            }
            root_sha
        } else if let Some(root_sha) = self.current_root_sha {
            root_sha
        } else {
            self.writer
                .write_object(PACK_OBJECT_TREE, &self.root_tree_cache)?
        };

        self.current_root_sha = Some(root_sha);
        self.tree_dirty = false;
        Ok(root_sha)
    }

    fn write_commit(
        &mut self,
        tree: [u8; 20],
        message: &str,
        author: GitPerson<'_>,
        committer: GitPerson<'_>,
        time: GitTimestamp,
    ) -> Result<[u8; 20]> {
        // Commit objects stay full-text because they are tiny and must exactly match Git's format.
        let tz = format_timezone_offset(time.offset_minutes);
        let mut commit = format!("tree {}\n", hex(&tree));
        if let Some(parent) = self.parent_commit {
            commit.push_str(&format!("parent {}\n", hex(&parent)));
        }
        commit.push_str(&format!(
            "author {} <{}> {} {tz}\n",
            author.name, author.email, time.epoch
        ));
        commit.push_str(&format!(
            "committer {} <{}> {} {tz}\n",
            committer.name, committer.email, time.epoch
        ));
        commit.push('\n');
        commit.push_str(message);
        self.writer
            .write_object(PACK_OBJECT_COMMIT, commit.as_bytes())
    }
}

impl PackWriter {
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

    fn write_object(&mut self, object_type: u8, data: &[u8]) -> Result<[u8; 20]> {
        //
        // Hash first so repeated trees/blobs/commits can be skipped entirely in the pack stream.
        //
        let sha = git_hash(object_type_name(object_type), data);
        if !self.seen.insert(sha) {
            return Ok(sha);
        }

        //
        // PACK object headers use a variable-length size encoding ahead of the compressed body.
        //
        let mut header = ((object_type & 0b111) << 4) | (data.len() as u8 & 0x0f);
        let mut remaining = data.len() >> 4;
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
        self.write_raw(&compress(data))?;
        self.object_count += 1;
        Ok(sha)
    }

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
        let mut header = (7u8 << 4) | (delta.len() as u8 & 0x0f);
        let mut remaining = delta.len() >> 4;
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
        self.write_raw(&base_sha)?;
        self.write_raw(&compress(delta))?;
        self.object_count += 1;
        Ok(result_sha)
    }

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

    fn write_raw(&mut self, bytes: &[u8]) -> Result<()> {
        self.body_file.write_all(bytes)?;
        Ok(())
    }
}

fn make_temp_output_path(output: &Path) -> Result<PathBuf> {
    let parent = output.parent().unwrap_or_else(|| Path::new("."));
    let name = output
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| anyhow!("invalid output path: {}", output.display()))?;
    Ok(parent.join(format!(".{name}.tmp-{}", process::id())))
}

fn init_bare_repo(repo_dir: &Path) -> Result<()> {
    let mut init_reftable = git_command();
    init_reftable
        .arg("init")
        .arg("--quiet")
        .arg("--bare")
        .arg("--initial-branch")
        .arg(MAIN_BRANCH)
        .arg("--ref-format")
        .arg("reftable")
        .arg(repo_dir);

    match init_reftable.output() {
        Ok(output) if output.status.success() => Ok(()),
        Ok(output) => {
            let reftable_stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
            let mut init_files = git_command();
            init_files
                .arg("init")
                .arg("--quiet")
                .arg("--bare")
                .arg("--initial-branch")
                .arg(MAIN_BRANCH)
                .arg(repo_dir);
            let output = init_files
                .output()
                .with_context(|| format!("failed to init bare repo at {}", repo_dir.display()))?;
            ensure_command_success(
                output,
                &format!(
                    "git init --bare fallback failed after reftable init error: {reftable_stderr}"
                ),
            )
        }
        Err(error) => Err(error)
            .with_context(|| format!("failed to init bare repo at {}", repo_dir.display())),
    }
}

fn git_command() -> Command {
    let mut command = Command::new("git");
    command.env("GIT_CONFIG_GLOBAL", "/dev/null");
    command.env("GIT_CONFIG_NOSYSTEM", "1");
    command.env_remove("GIT_DIR");
    command.env_remove("GIT_WORK_TREE");
    command
}

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

fn append_co_author_trailers(message: &str, co_authors: &[(&str, &str)]) -> String {
    if co_authors.is_empty() {
        return message.to_owned();
    }

    let mut rendered = String::from(message.trim_end());
    rendered.push_str("\n\n");
    for (index, (name, email)) in co_authors.iter().enumerate() {
        if index > 0 {
            rendered.push('\n');
        }
        rendered.push_str("Co-authored-by: ");
        rendered.push_str(name);
        rendered.push_str(" <");
        rendered.push_str(email);
        rendered.push('>');
    }
    rendered
}

fn ensure_repo_path(path: &str) -> Result<()> {
    if path.split('/').find(|part| !part.is_empty()).is_none() {
        bail!("invalid empty repository path");
    }
    Ok(())
}

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

fn tree_bytes(entries: &[Entry]) -> Vec<u8> {
    let mut tree = Vec::new();
    for entry in entries {
        tree.extend_from_slice(if entry.is_tree { b"40000 " } else { b"100644 " });
        tree.extend_from_slice(&entry.name);
        tree.push(0);
        tree.extend_from_slice(&entry.sha);
    }
    tree
}

fn tree_sort_cmp(
    left: &(&[u8], [u8; 20], bool),
    right: &(&[u8], [u8; 20], bool),
) -> std::cmp::Ordering {
    let common = left.0.len().min(right.0.len());
    match left.0[..common].cmp(&right.0[..common]) {
        std::cmp::Ordering::Equal => {
            let left_tail = if left.2 { b'/' } else { 0 };
            let right_tail = if right.2 { b'/' } else { 0 };
            let left_next = left.0.get(common).copied().unwrap_or(left_tail);
            let right_next = right.0.get(common).copied().unwrap_or(right_tail);
            left_next.cmp(&right_next)
        }
        other => other,
    }
}

fn compress(data: &[u8]) -> Vec<u8> {
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::fast());
    encoder
        .write_all(data)
        .expect("zlib write to Vec cannot fail");
    encoder.finish().expect("zlib finish on Vec cannot fail")
}

fn create_delta(src: &[u8], dst: &[u8]) -> Vec<u8> {
    //
    // Index fixed-size source blocks so destination scanning can prefer copy commands.
    //
    let mut delta = Vec::with_capacity(dst.len() / 2);
    encode_varint(&mut delta, src.len());
    encode_varint(&mut delta, dst.len());

    if src.len() < BLOCK_SIZE {
        emit_inserts(&mut delta, dst);
        return delta;
    }

    let mut index = HashMap::<u32, Vec<usize>>::new();
    for source_offset in (0..src.len().saturating_sub(BLOCK_SIZE - 1)).step_by(INDEX_STEP) {
        let hash = block_hash(&src[source_offset..source_offset + BLOCK_SIZE]);
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

        if remaining >= BLOCK_SIZE {
            let hash = block_hash(&dst[destination_offset..destination_offset + BLOCK_SIZE]);
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

        if best_len >= BLOCK_SIZE {
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

fn block_hash(data: &[u8]) -> u32 {
    let mut hash = 0x811c9dc5u32;
    for &byte in data {
        hash ^= byte as u32;
        hash = hash.wrapping_mul(0x01000193);
    }
    hash
}

fn match_length(src: &[u8], src_offset: usize, dst: &[u8], dst_offset: usize) -> usize {
    let max = std::cmp::min(src.len() - src_offset, dst.len() - dst_offset);
    let mut len = 0usize;
    while len < max && src[src_offset + len] == dst[dst_offset + len] {
        len += 1;
    }
    len
}

fn emit_inserts(out: &mut Vec<u8>, data: &[u8]) {
    let mut offset = 0usize;
    while offset < data.len() {
        let chunk_len = std::cmp::min(127, data.len() - offset);
        out.push(chunk_len as u8);
        out.extend_from_slice(&data[offset..offset + chunk_len]);
        offset += chunk_len;
    }
}

fn flush_inserts(out: &mut Vec<u8>, pending: &mut Vec<u8>) {
    if !pending.is_empty() {
        emit_inserts(out, pending);
        pending.clear();
    }
}

fn encode_varint(out: &mut Vec<u8>, mut value: usize) {
    while value >= 128 {
        out.push((value & 0x7f) as u8 | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

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

fn object_type_name(object_type: u8) -> &'static [u8] {
    match object_type {
        PACK_OBJECT_COMMIT => b"commit",
        PACK_OBJECT_TREE => b"tree",
        PACK_OBJECT_BLOB => b"blob",
        _ => panic!("invalid object type {object_type}"),
    }
}

fn hex(sha: &[u8; 20]) -> String {
    let mut encoded = String::with_capacity(40);
    for byte in sha {
        use std::fmt::Write as _;
        write!(encoded, "{byte:02x}").expect("formatting into String cannot fail");
    }
    encoded
}

fn format_timezone_offset(offset_minutes: i32) -> String {
    let sign = if offset_minutes < 0 { '-' } else { '+' };
    let total_minutes = offset_minutes.abs();
    let hours = total_minutes / 60;
    let minutes = total_minutes % 60;
    format!("{sign}{hours:02}{minutes:02}")
}

fn commit_time(promulgation_date: &str) -> Result<GitTimestamp> {
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

    let effective_date = if effective_date.len() != 10 {
        String::from("2000-01-01")
    } else if effective_date.as_str() < "1970-01-01" {
        String::from("1970-01-01")
    } else {
        effective_date
    };

    let year = effective_date[0..4].parse::<i32>()?;
    let month = effective_date[5..7].parse::<u8>()?;
    let day = effective_date[8..10].parse::<u8>()?;
    let month = Month::try_from(month)?;
    let date = Date::from_calendar_date(year, month, day)?;
    let datetime = PrimitiveDateTime::new(date, CivilTime::from_hms(12, 0, 0)?);
    let offset = UtcOffset::from_hms(9, 0, 0)?;
    Ok(GitTimestamp {
        epoch: datetime.assume_offset(offset).unix_timestamp(),
        offset_minutes: 9 * 60,
    })
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn clamps_pre_epoch_dates() {
        let temp = TempDir::new().unwrap();
        let output = temp.path().join("output.git");
        let mut writer = BareRepoWriter::create(&output).unwrap();
        writer
            .commit_law("kr/테스트법/법률.md", b"body", "message", "19491021")
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
            writer.commit_law("kr/테스트법/법률.md", b"body", "message", "20240101")?;
            writer.finish()?;
            Ok(())
        })();

        std::env::set_current_dir(previous_dir).unwrap();
        result.unwrap();

        let output = temp.path().join("output.git");
        assert_eq!(
            git_stdout(&output, ["rev-list", "--count", "HEAD"]).trim(),
            "1"
        );
    }

    #[test]
    fn root_tree_delta_handles_root_file_updates() {
        let temp = TempDir::new().unwrap();
        let output = temp.path().join("output.git");
        let mut writer = BareRepoWriter::create(&output).unwrap();
        writer
            .commit_static(
                "README.md",
                b"first\n",
                "initial commit",
                1_774_839_600,
                540,
            )
            .unwrap();
        writer
            .commit_static(
                "README.md",
                b"second\n",
                "update readme",
                1_774_839_601,
                540,
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
