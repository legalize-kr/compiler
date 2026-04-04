use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{BufWriter, Seek, SeekFrom, Write};
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

struct PackWriter {
    file: BufWriter<File>,
    object_count: u32,
    path: PathBuf,
    seen: HashSet<[u8; 20]>,
}

pub struct BareRepoWriter {
    writer: PackWriter,
    temp_output: PathBuf,
    final_output: PathBuf,
    root_files: Vec<Entry>,
    groups: Vec<Group>,
    parent_commit: Option<[u8; 20]>,
    current_root_sha: Option<[u8; 20]>,
    tree_dirty: bool,
}

impl BareRepoWriter {
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
            groups: Vec::new(),
            parent_commit: None,
            current_root_sha: None,
            tree_dirty: false,
        })
    }

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
        ensure_repo_path(path)?;
        let blob_sha = self.writer.write_object(PACK_OBJECT_BLOB, content)?;

        match path
            .split('/')
            .filter(|segment| !segment.is_empty())
            .collect::<Vec<_>>()
            .as_slice()
        {
            [name] => {
                upsert(&mut self.root_files, name.as_bytes(), blob_sha, false);
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
            }
            _ => bail!("unsupported repository path: {path}"),
        }

        self.tree_dirty = true;
        let root_sha = self.root_tree_sha()?;
        let commit_sha = self.write_commit(root_sha, message, author, committer, time)?;
        self.parent_commit = Some(commit_sha);
        Ok(())
    }

    fn ensure_group(&mut self, name: &[u8]) -> usize {
        if let Some(index) = self.groups.iter().position(|group| group.name == name) {
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
        position
    }

    fn root_tree_sha(&mut self) -> Result<[u8; 20]> {
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

        let kr_tree = if self.groups.is_empty() {
            None
        } else {
            let mut entries = Vec::with_capacity(self.groups.len());
            for group in &self.groups {
                entries.push(Entry {
                    name: group.name.clone(),
                    sha: group.cached_sha.context("missing cached subtree SHA")?,
                    is_tree: true,
                });
            }
            let tree = tree_bytes(&entries);
            Some(self.writer.write_object(PACK_OBJECT_TREE, &tree)?)
        };

        let mut root_entries =
            Vec::with_capacity(self.root_files.len() + usize::from(kr_tree.is_some()));
        for file in &self.root_files {
            root_entries.push((&file.name[..], file.sha, false));
        }
        if let Some(kr_tree) = kr_tree {
            root_entries.push((b"kr".as_slice(), kr_tree, true));
        }
        root_entries.sort_by(tree_sort_cmp);

        let mut root_tree = Vec::new();
        for (name, sha, is_tree) in root_entries {
            root_tree.extend_from_slice(if is_tree { b"40000 " } else { b"100644 " });
            root_tree.extend_from_slice(name);
            root_tree.push(0);
            root_tree.extend_from_slice(&sha);
        }

        let root_sha = self.writer.write_object(PACK_OBJECT_TREE, &root_tree)?;
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
        let file = BufWriter::with_capacity(1 << 20, File::create(path)?);
        let mut writer = Self {
            file,
            object_count: 0,
            path: path.to_path_buf(),
            seen: HashSet::new(),
        };
        writer.write_raw(b"PACK")?;
        writer.write_raw(&2u32.to_be_bytes())?;
        writer.write_raw(&0u32.to_be_bytes())?;
        Ok(writer)
    }

    fn write_object(&mut self, object_type: u8, data: &[u8]) -> Result<[u8; 20]> {
        let sha = git_hash(object_type_name(object_type), data);
        if !self.seen.insert(sha) {
            return Ok(sha);
        }

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

    fn finish(&mut self) -> Result<()> {
        self.file.flush()?;

        {
            let mut file = File::options().write(true).open(&self.path)?;
            file.seek(SeekFrom::Start(8))?;
            file.write_all(&self.object_count.to_be_bytes())?;
            file.flush()?;
        }

        let digest = sha::sha1(&fs::read(&self.path)?);
        File::options()
            .append(true)
            .open(&self.path)?
            .write_all(&digest)?;
        Ok(())
    }

    fn write_raw(&mut self, bytes: &[u8]) -> Result<()> {
        self.file.write_all(bytes)?;
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

fn upsert(entries: &mut Vec<Entry>, name: &[u8], sha: [u8; 20], is_tree: bool) {
    match entries.iter().position(|entry| entry.name == name) {
        Some(index) => entries[index].sha = sha,
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
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::new(6));
    encoder
        .write_all(data)
        .expect("zlib write to Vec cannot fail");
    encoder.finish().expect("zlib finish on Vec cannot fail")
}

fn git_hash(type_name: &[u8], data: &[u8]) -> [u8; 20] {
    let header = format!(
        "{} {}\0",
        std::str::from_utf8(type_name).expect("invalid object type name"),
        data.len()
    );
    let mut hasher = sha::Sha1::new();
    hasher.update(header.as_bytes());
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
