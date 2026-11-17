//! Scanning a static assets directory before any bucket mutation.
use anyhow::{Context, ensure};
use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::path::Path;
use wasmkeep_server::assets::{
    Bundle, Content, File, Index, MAX_FILE, MAX_FILES, MAX_TOTAL, digest, valid_name,
};

#[derive(Debug)]
pub(crate) struct Scan {
    pub bundle: Bundle,
    pub skipped: Vec<String>,
    pub warnings: Vec<String>,
}

impl Scan {
    pub(crate) fn print(&self) {
        for (name, file) in &self.bundle.index.files {
            println!(
                "  asset {name}: {} bytes, {}",
                file.identity.size, file.content_type
            );
        }
        for name in &self.skipped {
            println!("  skipped asset {name}");
        }
        for warning in &self.warnings {
            eprintln!("warning: {warning}");
        }
    }
}

fn skipped(name: &str) -> bool {
    !name.starts_with(".well-known/")
        && name != ".well-known"
        && name.split('/').any(|p| p.starts_with('.'))
}

#[derive(Default)]
struct Walk {
    files: BTreeMap<String, Vec<u8>>,
    skipped: Vec<String>,
    warnings: Vec<String>,
    errors: Vec<String>,
    total: u64,
}
impl Walk {
    fn take(&mut self, name: String, file: std::fs::File) -> anyhow::Result<()> {
        let size = file.metadata()?.len();
        ensure!(size <= MAX_FILE, "{name}: file exceeds 25 MiB");
        ensure!(self.files.len() < MAX_FILES, "more than 20,000 asset files");
        ensure!(self.total + size <= MAX_TOTAL, "assets exceed 512 MiB");
        let mut bytes = Vec::new();
        file.take(MAX_FILE + 1).read_to_end(&mut bytes)?;
        ensure!(
            bytes.len() as u64 <= MAX_FILE,
            "{name}: file exceeds 25 MiB"
        );
        self.total += bytes.len() as u64;
        ensure!(self.total <= MAX_TOTAL, "assets exceed 512 MiB");
        if !unicode_normalization::is_nfc(&name) {
            self.warnings.push(format!(
                "{name:?} is not Unicode NFC; asset names match as bytes"
            ));
        }
        self.files.insert(name, bytes);
        Ok(())
    }
}

// Every child is opened relative to an already-open directory, with
// NOFOLLOW. Replacing a directory or leaf with a symlink during the walk
// cannot turn the scan into a read outside DIR. Only DIR is resolved once.
#[cfg(unix)]
fn walk(dir: &std::fs::File, prefix: &str, out: &mut Walk) -> anyhow::Result<()> {
    use rustix::fs::{AtFlags, Dir, FileType, Mode, OFlags, openat, statat};
    for entry in Dir::read_from(dir)? {
        let entry = entry?;
        let raw = entry.file_name();
        if raw.to_bytes() == b"." || raw.to_bytes() == b".." {
            continue;
        }
        let Ok(part) = raw.to_str() else {
            out.errors.push(format!("{prefix}{raw:?}: invalid UTF-8"));
            continue;
        };
        let name = format!("{prefix}{part}");
        let result = (|| -> anyhow::Result<()> {
            ensure!(
                valid_name(&name),
                "{name:?}: invalid or reserved asset name"
            );
            let stat = statat(dir, raw, AtFlags::SYMLINK_NOFOLLOW)?;
            let kind = FileType::from_raw_mode(stat.st_mode);
            ensure!(
                matches!(kind, FileType::RegularFile | FileType::Directory),
                "{name:?}: symlink or non-regular file"
            );
            if skipped(&name) {
                out.skipped.push(name.clone());
                return Ok(());
            }
            let mut flags = OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK;
            if kind == FileType::Directory {
                flags |= OFlags::DIRECTORY;
            }
            let file = std::fs::File::from(openat(dir, raw, flags, Mode::empty())?);
            if kind == FileType::Directory {
                walk(&file, &format!("{name}/"), out)?;
            } else {
                ensure!(file.metadata()?.is_file(), "{name}: non-regular file");
                out.take(name.clone(), file)?;
            }
            Ok(())
        })();
        if let Err(error) = result {
            out.errors.push(format!("{name:?}: {error}"));
        }
    }
    Ok(())
}

#[cfg(not(unix))]
fn walk_path(dir: &Path, prefix: &str, out: &mut Walk) -> anyhow::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = format!(
            "{prefix}{}",
            entry
                .file_name()
                .to_str()
                .context("asset name is not UTF-8")?
        );
        ensure!(valid_name(&name), "invalid or reserved asset name {name:?}");
        let kind = entry.file_type()?;
        ensure!(
            kind.is_file() || kind.is_dir(),
            "{name}: symlink or non-regular file"
        );
        if skipped(&name) {
            out.skipped.push(name);
            continue;
        }
        if kind.is_dir() {
            walk_path(&entry.path(), &format!("{name}/"), out)?;
        } else {
            out.take(name, std::fs::File::open(entry.path())?)?;
        }
    }
    Ok(())
}

pub(crate) fn scan(dir: &Path, mut rules: Index) -> anyhow::Result<Scan> {
    rules.validate_rules().map_err(anyhow::Error::msg)?;
    rules.files.clear();
    let root = dir
        .canonicalize()
        .with_context(|| format!("assets directory {}", dir.display()))?;
    ensure!(root.is_dir(), "assets DIR must be a directory");
    let mut found = Walk::default();
    #[cfg(unix)]
    walk(&std::fs::File::open(&root)?, "", &mut found)?;
    #[cfg(not(unix))]
    walk_path(&root, "", &mut found)?;
    ensure!(
        found.errors.is_empty(),
        "assets refused:\n{}",
        found.errors.join("\n")
    );
    let mut contents = BTreeMap::new();
    for (name, bytes) in &found.files {
        if [".br", ".gz"].iter().any(|ext| {
            name.strip_suffix(ext)
                .is_some_and(|base| found.files.contains_key(base))
        }) {
            continue;
        }
        let mut add = |bytes: Vec<u8>| {
            let hash = digest(&bytes);
            let size = bytes.len() as u64;
            contents.insert(hash.clone(), bytes);
            Content { digest: hash, size }
        };
        let (mime, compress) = content_type(name);
        let mut file = File {
            content_type: mime.into(),
            identity: add(bytes.clone()),
            br: None,
            gzip: None,
        };
        for (ext, is_br) in [("br", true), ("gz", false)] {
            let variant = match found.files.get(&format!("{name}.{ext}")) {
                Some(compressed) => {
                    let reader: Box<dyn Read> = if is_br {
                        Box::new(brotli::Decompressor::new(compressed.as_slice(), 4096))
                    } else {
                        Box::new(flate2::read::MultiGzDecoder::new(compressed.as_slice()))
                    };
                    let mut decoded = Vec::new();
                    reader
                        .take(bytes.len() as u64 + 1)
                        .read_to_end(&mut decoded)
                        .with_context(|| format!("{name}.{ext}: invalid compressed variant"))?;
                    ensure!(
                        &decoded == bytes,
                        "{name}.{ext}: variant does not decompress to {name}"
                    );
                    Some(compressed.clone())
                }
                None if compress => {
                    let compressed = if is_br {
                        let mut out = Vec::new();
                        {
                            let mut writer = brotli::CompressorWriter::new(&mut out, 4096, 5, 22);
                            writer.write_all(bytes)?;
                        }
                        out
                    } else {
                        let mut writer = flate2::GzBuilder::new()
                            .mtime(0)
                            .write(Vec::new(), flate2::Compression::default());
                        writer.write_all(bytes)?;
                        writer.finish()?
                    };
                    (compressed.len() * 10 <= bytes.len() * 9).then_some(compressed)
                }
                None => None,
            };
            if is_br {
                file.br = variant.map(&mut add);
            } else {
                file.gzip = variant.map(&mut add);
            }
        }
        rules.files.insert(name.clone(), file);
    }
    rules.validate().map_err(anyhow::Error::msg)?;
    found.skipped.sort();
    found.warnings.sort();
    Ok(Scan {
        bundle: Bundle {
            index: rules,
            contents,
        },
        skipped: found.skipped,
        warnings: found.warnings,
    })
}

// Fixed deploy-time table; persisted types never depend on a later CLI.
fn content_type(name: &str) -> (&'static str, bool) {
    match name
        .rsplit('.')
        .next()
        .unwrap_or("")
        .to_ascii_lowercase()
        .as_str()
    {
        "html" | "htm" => ("text/html;charset=utf-8", true),
        "css" => ("text/css;charset=utf-8", true),
        "js" | "mjs" => ("text/javascript;charset=utf-8", true),
        "json" | "map" => ("application/json;charset=utf-8", true),
        "svg" => ("image/svg+xml;charset=utf-8", true),
        "xml" => ("application/xml;charset=utf-8", true),
        "txt" => ("text/plain;charset=utf-8", true),
        "wasm" => ("application/wasm", true),
        "png" => ("image/png", false),
        "jpg" | "jpeg" => ("image/jpeg", false),
        "gif" => ("image/gif", false),
        "webp" => ("image/webp", false),
        "avif" => ("image/avif", false),
        "ico" => ("image/x-icon", false),
        "woff" => ("font/woff", false),
        "woff2" => ("font/woff2", false),
        "pdf" => ("application/pdf", false),
        "zip" => ("application/zip", false),
        _ => ("application/octet-stream", false),
    }
}
#[cfg(test)]
#[path = "assets_tests.rs"]
mod tests;
