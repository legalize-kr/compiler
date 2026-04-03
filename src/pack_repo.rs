/*
 * Direct packfile writer for bare git repositories.
 *
 * Generates a single packfile instead of loose objects.
 * Supports nested tree paths (e.g., kr/group/file.md).
 *
 * Each commit updates one blob in one group subtree.
 * Only the changed subtree is re-serialized; all others
 * subtrees keep their SHA.
 */

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result};
use sha1::{Digest, Sha1};

const BOT_NAME: &str = "legalize-kr-bot";
const BOT_EMAIL: &str = "bot@legalize.kr";
const INITIAL_COMMIT_AUTHOR: &str = "Junghwan Park <reserve.dev@gmail.com>";
const INITIAL_COMMIT_COMMITTER: &str = "Jihyeon Kim <simnalamburt@gmail.com>";
const INITIAL_COMMIT_CO_AUTHORS: &[(&str, &str)] = &[("Jihyeon Kim", "simnalamburt@gmail.com")];

struct Entry {
    name: Vec<u8>,
    sha: [u8; 20],
    is_tree: bool,
}

struct Group {
    name: Vec<u8>,
    files: Vec<Entry>,
    cached_sha: Option<[u8; 20]>,
}

pub struct PackRepoWriter {
    pw: PackWriter,
    root_files: Vec<Entry>,
    groups: Vec<Group>,
    parent: Option<[u8; 20]>,
    output: PathBuf,
    /* directory tree cache for REF_DELTA */
    dir_tree_cache: Vec<u8>,
    dir_tree_sha_offsets: Vec<usize>,
    dir_tree_prev_sha: Option<[u8; 20]>,
    /* blob cache: path → (sha, content) for blob delta chains */
    prev_blobs: HashMap<String, ([u8; 20], Vec<u8>)>,
}

impl PackRepoWriter {
    pub fn create(output: &Path) -> Result<Self> {
        if output.exists() {
            fs::remove_dir_all(output)?;
        }
        let r = Command::new("git").args(["init", "--bare"]).arg(output)
            .output().context("git init")?;
        if !r.status.success() {
            anyhow::bail!("git init: {}", String::from_utf8_lossy(&r.stderr));
        }
        let pp = output.join("objects/pack/tmp_pack.pack");
        fs::create_dir_all(pp.parent().unwrap())?;

        Ok(Self {
            pw: PackWriter::new(&pp)?,
            root_files: Vec::new(),
            groups: Vec::new(),
            parent: None,
            output: output.to_path_buf(),
            dir_tree_cache: Vec::new(),
            dir_tree_sha_offsets: Vec::new(),
            dir_tree_prev_sha: None,
            prev_blobs: HashMap::new(),
        })
    }

    pub fn commit_law(
        &mut self, path: &str, md: &[u8], msg: &str, prom_date: &str,
    ) -> Result<()> {
        let (epoch, tz) = commit_time(prom_date);
        self.commit(path, md, msg, epoch, tz)
    }

    pub fn commit_static(
        &mut self, path: &str, data: &[u8], msg: &str, epoch: i64, tz: i32,
    ) -> Result<()> {
        let mut full_msg = String::from(msg);
        for (name, email) in INITIAL_COMMIT_CO_AUTHORS {
            full_msg.push_str(&format!("\n\nCo-authored-by: {name} <{email}>"));
        }
        self.commit_with_author(path, data, &full_msg, epoch, tz,
                                Some(INITIAL_COMMIT_AUTHOR.to_owned()),
                                Some(INITIAL_COMMIT_COMMITTER.to_owned()))
    }

    pub fn commit(
        &mut self, path: &str, content: &[u8], msg: &str, epoch: i64, tz: i32,
    ) -> Result<()> {
        self.commit_with_author(path, content, msg, epoch, tz, None, None)
    }

    fn commit_with_author(
        &mut self, path: &str, content: &[u8], msg: &str, epoch: i64, tz: i32,
        author_override: Option<String>, committer_override: Option<String>,
    ) -> Result<()> {
        let blob_sha = git_hash(b"blob", content);

        /* Try blob delta against previous version of same path */
        if let Some((prev_sha, prev_content)) = self.prev_blobs.get(path) {
            let delta = create_delta(prev_content, content);
            if delta.len() < content.len() * 3 / 4 {
                /* delta is worthwhile (saves >= 25%) */
                self.pw.write_ref_delta(*prev_sha, &delta)?;
            } else {
                self.pw.write_obj(3, content)?;
            }
        } else {
            self.pw.write_obj(3, content)?;
        }
        self.prev_blobs.insert(path.to_owned(), (blob_sha, content.to_vec()));

        let parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

        match parts.len() {
            1 => {
                upsert(&mut self.root_files, parts[0].as_bytes(), blob_sha, false);
            }
            3 if parts[0] == "kr" => {
                let gname = parts[1].as_bytes();
                let fname = parts[2].as_bytes();
                let gi = self.ensure_group(gname);
                upsert(&mut self.groups[gi].files, fname, blob_sha, false);
                self.groups[gi].cached_sha = None; /* invalidate */
            }
            _ => anyhow::bail!("unsupported path: {path}"),
        }

        let root_sha = self.build_root_tree()?;
        let c = self.make_commit(root_sha, msg, epoch, tz,
                                 author_override.as_deref(),
                                 committer_override.as_deref())?;
        self.parent = Some(c);
        Ok(())
    }

    pub fn finish(mut self) -> Result<()> {
        self.pw.finish()?;

        if let Some(sha) = self.parent {
            let rd = self.output.join("refs/heads");
            fs::create_dir_all(&rd)?;
            fs::write(rd.join("main"), format!("{}\n", hex(&sha)))?;
        }
        fs::write(self.output.join("HEAD"), "ref: refs/heads/main\n")?;

        let pp = self.output.join("objects/pack/tmp_pack.pack");
        let r = Command::new("git").arg("index-pack").arg(&pp)
            .output().context("git index-pack")?;
        if !r.status.success() {
            anyhow::bail!("index-pack: {}", String::from_utf8_lossy(&r.stderr));
        }
        Ok(())
    }

    fn ensure_group(&mut self, name: &[u8]) -> usize {
        if let Some(i) = self.groups.iter().position(|g| g.name == name) {
            return i;
        }
        let pos = self.groups.partition_point(|g| g.name.as_slice() < name);
        self.groups.insert(pos, Group {
            name: name.to_vec(),
            files: Vec::new(),
            cached_sha: None,
        });
        pos
    }

    fn build_root_tree(&mut self) -> Result<[u8; 20]> {
        /* 1. Ensure every group has a cached subtree SHA */
        for g in &mut self.groups {
            if g.cached_sha.is_some() {
                continue;
            }
            let buf = tree_bytes(&g.files);
            let sha = git_hash(b"tree", &buf);
            self.pw.write_obj(2, &buf)?;
            g.cached_sha = Some(sha);
        }

        /* 2. Build directory tree, using REF_DELTA when possible.
         *    Delta is valid when no new groups were added (same structure). */
        let dir_sha;
        let can_delta = self.dir_tree_prev_sha.is_some()
            && self.dir_tree_sha_offsets.len() == self.groups.len();

        if can_delta {
            /* Find which group changed by comparing against dir_tree_cache */
            let mut changed_idx = None;
            for (i, g) in self.groups.iter().enumerate() {
                let off = self.dir_tree_sha_offsets[i];
                let new = g.cached_sha.unwrap();
                if self.dir_tree_cache[off..off + 20] != new {
                    changed_idx = Some((i, off, new));
                    break;
                }
            }

            if let Some((_, off, new_sha)) = changed_idx {
                /* Build delta BEFORE patching cache */
                let delta = make_copy_insert_delta(
                    self.dir_tree_cache.len(), off, &new_sha,
                );
                /* Now patch cache */
                self.dir_tree_cache[off..off + 20].copy_from_slice(&new_sha);
                dir_sha = git_hash(b"tree", &self.dir_tree_cache);
                self.pw.write_ref_delta(self.dir_tree_prev_sha.unwrap(), &delta)?;
            } else {
                /* Nothing changed in kr/ tree (shouldn't happen, but safe) */
                dir_sha = self.dir_tree_prev_sha.unwrap();
            }
        } else {
            /* Full serialize + record offsets for future deltas */
            self.dir_tree_cache.clear();
            self.dir_tree_sha_offsets.clear();
            for g in &self.groups {
                self.dir_tree_cache.extend_from_slice(b"40000 ");
                self.dir_tree_cache.extend_from_slice(&g.name);
                self.dir_tree_cache.push(0);
                self.dir_tree_sha_offsets.push(self.dir_tree_cache.len());
                self.dir_tree_cache.extend_from_slice(&g.cached_sha.unwrap());
            }
            dir_sha = git_hash(b"tree", &self.dir_tree_cache);
            self.pw.write_obj(2, &self.dir_tree_cache)?;
        }
        self.dir_tree_prev_sha = Some(dir_sha);

        /* 3. Root tree: root_files + kr directory, sorted by git rules */
        let mut root = Vec::<(&[u8], [u8; 20], bool)>::new();
        for e in &self.root_files {
            root.push((&e.name, e.sha, false));
        }
        if !self.groups.is_empty() {
            root.push((b"kr", dir_sha, true));
        }
        root.sort_by(tree_sort_cmp);

        let mut root_buf = Vec::new();
        for (name, sha, is_tree) in &root {
            root_buf.extend_from_slice(if *is_tree { b"40000 " } else { b"100644 " });
            root_buf.extend_from_slice(name);
            root_buf.push(0);
            root_buf.extend_from_slice(sha);
        }
        let root_sha = git_hash(b"tree", &root_buf);
        self.pw.write_obj(2, &root_buf)?;
        Ok(root_sha)
    }

    fn make_commit(
        &mut self, tree: [u8; 20], msg: &str, epoch: i64, tz: i32,
        author_override: Option<&str>, committer_override: Option<&str>,
    ) -> Result<[u8; 20]> {
        let sign = if tz >= 0 { '+' } else { '-' };
        let a = tz.unsigned_abs();
        let tz_str = format!("{sign}{:02}{:02}", a / 60, a % 60);
        let default_id = format!("{BOT_NAME} <{BOT_EMAIL}>");
        let author_id = author_override.unwrap_or(&default_id);
        let committer_id = committer_override.unwrap_or(&default_id);

        let mut buf = format!("tree {}\n", hex(&tree));
        if let Some(p) = self.parent {
            buf.push_str(&format!("parent {}\n", hex(&p)));
        }
        buf.push_str(&format!("author {author_id} {epoch} {tz_str}\n"));
        buf.push_str(&format!("committer {committer_id} {epoch} {tz_str}\n"));
        buf.push_str(&format!("\n{msg}"));
        self.pw.write_obj(1, buf.as_bytes())
    }
}

/* --- helpers --- */

fn upsert(v: &mut Vec<Entry>, name: &[u8], sha: [u8; 20], is_tree: bool) {
    match v.iter().position(|e| e.name == name) {
        Some(i) => v[i].sha = sha,
        None => {
            let p = v.partition_point(|e| e.name.as_slice() < name);
            v.insert(p, Entry { name: name.to_vec(), sha, is_tree });
        }
    }
}

fn tree_bytes(entries: &[Entry]) -> Vec<u8> {
    let mut buf = Vec::new();
    for e in entries {
        buf.extend_from_slice(if e.is_tree { b"40000 " } else { b"100644 " });
        buf.extend_from_slice(&e.name);
        buf.push(0);
        buf.extend_from_slice(&e.sha);
    }
    buf
}

fn tree_sort_cmp(a: &(&[u8], [u8; 20], bool), b: &(&[u8], [u8; 20], bool)) -> std::cmp::Ordering {
    /* git sorts tree entries by name, with '/' appended to directories */
    let ak: &[u8] = a.0;
    let bk: &[u8] = b.0;
    let common = std::cmp::min(ak.len(), bk.len());
    match ak[..common].cmp(&bk[..common]) {
        std::cmp::Ordering::Equal => {
            let a_tail = if a.2 { b'/' } else { 0 };
            let b_tail = if b.2 { b'/' } else { 0 };
            let a_next = ak.get(common).copied().unwrap_or(a_tail);
            let b_next = bk.get(common).copied().unwrap_or(b_tail);
            a_next.cmp(&b_next)
        }
        other => other,
    }
}

fn git_hash(typename: &[u8], data: &[u8]) -> [u8; 20] {
    let hdr = format!("{} {}\0", std::str::from_utf8(typename).unwrap(), data.len());
    let mut h = Sha1::new();
    h.update(hdr.as_bytes());
    h.update(data);
    h.finalize().into()
}

fn hex(sha: &[u8; 20]) -> String {
    let mut s = String::with_capacity(40);
    for b in sha {
        use std::fmt::Write;
        write!(s, "{b:02x}").unwrap();
    }
    s
}

fn compress(data: &[u8]) -> Vec<u8> {
    use flate2::write::ZlibEncoder;
    use flate2::Compression;
    let mut e = ZlibEncoder::new(Vec::new(), Compression::new(6));
    /* Vec write only fails on OOM, which aborts anyway */
    e.write_all(data).expect("zlib write");
    e.finish().expect("zlib finish")
}

/*
 * Binary delta: find matching blocks between src and dst using a hash index.
 * Returns git pack delta format: varint(src_size) + varint(dst_size) + instructions.
 *
 * Uses 16-byte block fingerprinting (same approach as git's diff-delta.c).
 */
const BLOCK_SIZE: usize = 16;
const INDEX_STEP: usize = 16; /* step=1: 320MB->305MB but 3:37->11:28 (81k commits) */

fn create_delta(src: &[u8], dst: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(dst.len() / 2);
    encode_varint(&mut out, src.len());
    encode_varint(&mut out, dst.len());

    if src.len() < BLOCK_SIZE {
        emit_inserts(&mut out, dst);
        return out;
    }

    /* Build hash index of source at every INDEX_STEP bytes */
    let mut index: HashMap<u32, Vec<usize>> = HashMap::new();
    for i in (0..src.len().saturating_sub(BLOCK_SIZE - 1)).step_by(INDEX_STEP) {
        let h = block_hash(&src[i..i + BLOCK_SIZE]);
        index.entry(h).or_default().push(i);
    }

    let mut dpos: usize = 0;
    let mut pending: Vec<u8> = Vec::new();

    while dpos < dst.len() {
        let remaining = dst.len() - dpos;
        let mut best_soff: usize = 0;
        let mut best_len: usize = 0;

        if remaining >= BLOCK_SIZE {
            let h = block_hash(&dst[dpos..dpos + BLOCK_SIZE]);
            if let Some(positions) = index.get(&h) {
                for &soff in positions {
                    /* verify match and extend */
                    let mlen = match_length(src, soff, dst, dpos);
                    if mlen > best_len {
                        best_len = mlen;
                        best_soff = soff;
                    }
                }
            }
        }

        if best_len >= BLOCK_SIZE {
            /* flush pending inserts, then emit copy */
            flush_inserts(&mut out, &mut pending);
            emit_copy(&mut out, best_soff, best_len);
            dpos += best_len;
        } else {
            pending.push(dst[dpos]);
            dpos += 1;
        }
    }

    flush_inserts(&mut out, &mut pending);
    out
}

fn block_hash(data: &[u8]) -> u32 {
    /* FNV-1a 32-bit */
    let mut h: u32 = 0x811c9dc5;
    for &b in data {
        h ^= b as u32;
        h = h.wrapping_mul(0x01000193);
    }
    h
}

fn match_length(src: &[u8], soff: usize, dst: &[u8], doff: usize) -> usize {
    let max = std::cmp::min(src.len() - soff, dst.len() - doff);
    let mut len = 0;
    while len < max && src[soff + len] == dst[doff + len] {
        len += 1;
    }
    len
}

fn emit_inserts(out: &mut Vec<u8>, data: &[u8]) {
    let mut pos = 0;
    while pos < data.len() {
        let chunk = std::cmp::min(127, data.len() - pos);
        out.push(chunk as u8);
        out.extend_from_slice(&data[pos..pos + chunk]);
        pos += chunk;
    }
}

fn flush_inserts(out: &mut Vec<u8>, pending: &mut Vec<u8>) {
    if !pending.is_empty() {
        emit_inserts(out, pending);
        pending.clear();
    }
}

/*
 * Generate a git pack delta: copy(0..off) + insert(20 bytes) + copy(off+20..end).
 * Source and destination have the same length; only 20 bytes at `off` differ.
 */
fn make_copy_insert_delta(total: usize, off: usize, new_sha: &[u8; 20]) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    encode_varint(&mut out, total);
    encode_varint(&mut out, total);

    /* Copy [0, off) from source */
    if off > 0 {
        emit_copy(&mut out, 0, off);
    }

    /* Insert 20 new SHA bytes */
    out.push(20);
    out.extend_from_slice(new_sha);

    /* Copy [off+20, total) from source */
    let tail_off = off + 20;
    let tail_sz = total - tail_off;
    if tail_sz > 0 {
        emit_copy(&mut out, tail_off, tail_sz);
    }
    out
}

/* Emit a git pack delta copy instruction: copy `size` bytes from source at `offset`. */
fn emit_copy(out: &mut Vec<u8>, offset: usize, size: usize) {
    let mut cmd: u8 = 0x80;
    let mut args = Vec::with_capacity(7);
    if offset & 0xff != 0       { cmd |= 0x01; args.push((offset & 0xff) as u8); }
    if offset & 0xff00 != 0     { cmd |= 0x02; args.push(((offset >> 8) & 0xff) as u8); }
    if offset & 0xff0000 != 0   { cmd |= 0x04; args.push(((offset >> 16) & 0xff) as u8); }
    if offset & 0xff000000 != 0 { cmd |= 0x08; args.push(((offset >> 24) & 0xff) as u8); }
    if size & 0xff != 0         { cmd |= 0x10; args.push((size & 0xff) as u8); }
    if size & 0xff00 != 0       { cmd |= 0x20; args.push(((size >> 8) & 0xff) as u8); }
    if size & 0xff0000 != 0     { cmd |= 0x40; args.push(((size >> 16) & 0xff) as u8); }
    out.push(cmd);
    out.extend_from_slice(&args);
}

fn encode_varint(out: &mut Vec<u8>, mut v: usize) {
    while v >= 128 {
        out.push((v & 0x7f) as u8 | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn commit_time(date: &str) -> (i64, i32) {
    let d = if date.len() == 8 && date.bytes().all(|b| b.is_ascii_digit()) && date >= "19700101" {
        date
    } else if date.len() == 8 && date.bytes().all(|b| b.is_ascii_digit()) {
        "19700101"
    } else {
        "20000101"
    };
    let y: i32 = d[0..4].parse().unwrap_or(2000);
    let m: u32 = d[4..6].parse().unwrap_or(1);
    let day: u32 = d[6..8].parse().unwrap_or(1);
    (days_since_epoch(y, m, day) * 86400 + 3 * 3600, 540)
}

fn days_since_epoch(y: i32, m: u32, d: u32) -> i64 {
    let (mut y, mut m) = (y as i64, m as i64);
    if m <= 2 { y -= 1; m += 12; }
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (m - 3) + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

/* --- packfile writer --- */

struct PackWriter {
    f: BufWriter<File>,
    n: u32,
    path: PathBuf,
}

impl PackWriter {
    fn new(path: &Path) -> Result<Self> {
        let f = BufWriter::with_capacity(1 << 20, File::create(path)?);
        let mut w = Self { f, n: 0, path: path.to_path_buf() };
        w.raw(b"PACK")?;
        w.raw(&2u32.to_be_bytes())?;
        w.raw(&0u32.to_be_bytes())?;
        Ok(w)
    }

    fn raw(&mut self, d: &[u8]) -> Result<()> {
        self.f.write_all(d)?;
        Ok(())
    }

    fn write_obj(&mut self, otype: u8, data: &[u8]) -> Result<[u8; 20]> {
        let sha = git_hash(type_str(otype), data);
        let sz = data.len();
        let mut hdr = ((otype & 7) << 4) | (sz & 0xf) as u8;
        let mut rem = sz >> 4;
        if rem > 0 { hdr |= 0x80; }
        self.raw(&[hdr])?;
        while rem > 0 {
            let mut b = (rem & 0x7f) as u8;
            rem >>= 7;
            if rem > 0 { b |= 0x80; }
            self.raw(&[b])?;
        }
        self.raw(&compress(data))?;
        self.n += 1;
        Ok(sha)
    }

    fn write_ref_delta(&mut self, base: [u8; 20], delta: &[u8]) -> Result<()> {
        let sz = delta.len();
        let mut hdr = (7u8 << 4) | (sz & 0xf) as u8;
        let mut rem = sz >> 4;
        if rem > 0 { hdr |= 0x80; }
        self.raw(&[hdr])?;
        while rem > 0 {
            let mut b = (rem & 0x7f) as u8;
            rem >>= 7;
            if rem > 0 { b |= 0x80; }
            self.raw(&[b])?;
        }
        self.raw(&base)?;
        self.raw(&compress(delta))?;
        self.n += 1;
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        self.f.flush()?;
        let cnt = self.n.to_be_bytes();
        {
            let mut f = File::options().write(true).open(&self.path)?;
            f.seek(SeekFrom::Start(8))?;
            f.write_all(&cnt)?;
            f.flush()?;
            /* drop f before re-reading the file */
        }
        let data = fs::read(&self.path)?;
        let cksum: [u8; 20] = { let mut h = Sha1::new(); h.update(&data); h.finalize().into() };
        fs::OpenOptions::new().append(true).open(&self.path)?.write_all(&cksum)?;
        Ok(())
    }
}

fn type_str(t: u8) -> &'static [u8] {
    match t { 1 => b"commit", 2 => b"tree", 3 => b"blob", _ => panic!("invalid object type {t}") }
}
