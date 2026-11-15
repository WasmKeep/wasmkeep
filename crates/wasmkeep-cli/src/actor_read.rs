//! Read-only operator data access (ADR 0033).
use anyhow::Context;
use clap::{Args, ValueEnum};
use std::io::{IsTerminal, Write};
use std::path::{Path, PathBuf};
use wasmkeep_server::history::{self, Point};
use wasmkeep_storage::{Rows, SqlValue};

#[derive(Debug, Args)]
pub(crate) struct Source {
    /// Actor's app, TENANT/APP.
    #[arg(value_name = "TENANT/APP", value_parser = crate::parse_app)]
    app: (String, String),
    /// Actor address, CLASS/NAME.
    actor: String,
    /// s3://BUCKET[/PREFIX]; endpoint, region and credentials from AWS_*.
    #[arg(long)]
    bucket: String,
    /// Capture-time boundary: RFC3339 or relative -15m, -2h, -1d.
    /// Uses LTX stamps, not upload times; only the retained current ancestry.
    #[arg(long, allow_hyphen_values = true, conflicts_with = "txid")]
    at: Option<String>,
    /// Exact retained transaction boundary on the current ancestry.
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    txid: Option<u64>,
    /// Maximum replayed database bytes (twice the default server cap).
    #[arg(long, default_value_t = 2 * 1024 * 1024 * 1024u64, value_parser = clap::value_parser!(u64).range(65536..))]
    max_bytes: u64,
    /// Parent directory for the plaintext scratch copy.
    #[arg(long, value_name = "DIR")]
    tmpdir: Option<PathBuf>,
    /// Refuse an unavailable or mismatched owner hint (exit 3).
    #[arg(long)]
    require_owner_match: bool,
}

#[derive(Debug, Args)]
pub(crate) struct ExportArgs {
    #[command(flatten)]
    source: Source,
    /// New output SQLite file; existing files are never overwritten.
    #[arg(short, long, value_name = "FILE")]
    output: PathBuf,
}

#[derive(Debug, Args)]
pub(crate) struct SqlArgs {
    #[command(flatten)]
    source: Source,
    /// One read-only statement. Host/schema tables and PRAGMAs are denied.
    query: String,
    /// Output format (default: table on a terminal, csv otherwise).
    #[arg(long, value_enum)]
    format: Option<Format>,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Format {
    Table,
    Csv,
    Tsv,
    Json,
    Jsonl,
}

pub(crate) fn export(args: &ExportArgs) -> anyhow::Result<()> {
    run(async {
        let result = read(&args.source, &args.output).await?;
        report(&result);
        println!(
            "Exported {} ({} bytes).",
            args.output.display(),
            result.bytes
        );
        Ok(())
    })
}

pub(crate) fn sql(args: &SqlArgs) -> anyhow::Result<()> {
    run(async {
        let dir = match &args.source.tmpdir {
            Some(parent) => tempfile::tempdir_in(parent)?,
            None => tempfile::tempdir()?,
        };
        let path = dir.path().join("actor.db");
        let result = read(&args.source, &path).await?;
        report(&result);
        let query = args.query.clone();
        let format = args.format;
        tokio::task::spawn_blocking(move || {
            let rows = wasmkeep_storage::readonly::query(&path, &query)?;
            let format = format.unwrap_or_else(|| {
                if std::io::stdout().is_terminal() {
                    Format::Table
                } else {
                    Format::Csv
                }
            });
            // The temporary directory lives until output completes, including errors.
            render(&rows, format, &mut std::io::stdout().lock())
        })
        .await?
    })
}

async fn read(source: &Source, output: &Path) -> anyhow::Result<history::Export> {
    let (tenant, app) = &source.app;
    let (_, class, name) = crate::move_target(&source.actor, Some(&source.app))?;
    let actor = wasmkeep_runtime::ActorAddress::new(tenant, class, name)?;
    // Validate the app identifier with the same grammar as a tenant/class.
    wasmkeep_runtime::ActorAddress::new(tenant, app, "validation")?;
    let point = if let Some(at) = &source.at {
        Point::At(parse_at(at)?)
    } else if let Some(txid) = source.txid {
        Point::Txid(txid)
    } else {
        Point::Head
    };
    // Same builder and AWS_* environment as other CLI bucket commands. This
    // reader needs only history plus optional ownership access, no control GET.
    let (store, prefix) = crate::open_bucket_unchecked(&source.bucket)?;
    let root = wasmkeep_cluster::state_root::StateRoot::new(&prefix, tenant, app);
    history::export_with_options(
        &store,
        &root,
        &actor,
        point,
        output,
        history::Options {
            max_bytes: source.max_bytes,
            tmpdir: source.tmpdir.as_deref(),
            require_owner_match: source.require_owner_match,
        },
    )
    .await
    .map_err(Into::into)
}

// Returning from select drops the export future and its TempDirs before exit.
fn run(work: impl std::future::Future<Output = anyhow::Result<()>>) -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let result = runtime.block_on(async {
        #[cfg(unix)]
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        let interrupt = tokio::signal::ctrl_c();
        let terminate = async {
            #[cfg(unix)]
            term.recv().await;
            #[cfg(not(unix))]
            std::future::pending::<()>().await;
        };
        tokio::select! {
            biased;
            signal = interrupt => { signal?; anyhow::bail!("interrupted; scratch copy removed") },
            () = terminate => anyhow::bail!("terminated; scratch copy removed"),
            result = work => result,
        }
    });
    // A cancelled SQL/output worker must not hold up signal-driven exit.
    runtime.shutdown_background();
    result
}

pub(crate) fn finish(result: anyhow::Result<()>) -> anyhow::Result<()> {
    if let Err(error) = &result
        && failure_code(error) == 3
    {
        eprintln!("Error: {error}");
        std::process::exit(3);
    }
    result
}

fn failure_code(error: &anyhow::Error) -> i32 {
    if matches!(
        error.downcast_ref::<history::Error>(),
        Some(history::Error::OwnerMismatch)
    ) {
        3
    } else {
        1
    }
}

fn parse_at(text: &str) -> anyhow::Result<i64> {
    if let Some(relative) = text.strip_prefix('-') {
        let (split, _) = relative
            .char_indices()
            .next_back()
            .context("--at needs RFC3339 or -15m/-2h/-1d")?;
        let (number, unit) = relative.split_at(split);
        let amount: i64 = number.parse().context("invalid relative --at amount")?;
        anyhow::ensure!(amount >= 0, "relative --at must be in the past");
        let multiplier = match unit {
            "s" => 1000,
            "m" => 60_000,
            "h" => 3_600_000,
            "d" => 86_400_000,
            _ => anyhow::bail!("relative --at units: s, m, h, d"),
        };
        let ago = amount
            .checked_mul(multiplier)
            .context("relative --at is too large")?;
        let now = i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_millis(),
        )?;
        return now.checked_sub(ago).context("relative --at is too large");
    }
    Ok(chrono::DateTime::parse_from_rfc3339(text)
        .context("--at needs an RFC3339 time with timezone, or -15m/-2h/-1d")?
        .timestamp_millis())
}

fn report(result: &history::Export) {
    let stamp = chrono::DateTime::from_timestamp_millis(result.timestamp_ms)
        .map_or_else(|| result.timestamp_ms.to_string(), |t| t.to_rfc3339());
    eprintln!(
        "generation {} txid {}; captured {stamp}; {} of listed history; raw checksum {:016x}",
        result.position.generation,
        result.position.txid,
        if result.position == result.listed_head {
            "head"
        } else {
            "historical point"
        },
        result.raw_checksum
    );
    match result.owner_generation {
        Some(g) if g == result.listed_head.generation => eprintln!(
            "Owner generation matches ({g}); this is a hint, not proof of acknowledgement. The newest segment may still be an unacknowledged tip."
        ),
        Some(g) => eprintln!(
            "Warning: newest segment generation {} differs from owner generation {g}; it may be an unacknowledged tip, overtaken by later history.",
            result.listed_head.generation
        ),
        None => eprintln!(
            "Warning: owner generation unavailable; the newest segment may be an unacknowledged tip, overtaken by later history."
        ),
    }
    eprintln!(
        "LTX trailers, checksum chain, final checksum and integrity_check: ok; {} foreign-key violations; {} objects read in successful attempt, {} attempt(s); app tables: {}",
        result.verification.foreign_key_violations,
        result.objects,
        result.attempts,
        result.verification.tables.join(", ")
    );
    if let Some(label) = &result.verification.replaced {
        eprintln!("Replacement: {label}");
    } else if result.verification.tables.is_empty() {
        eprintln!(
            "Probably deleted: no application tables (no replacement label from this writer)."
        );
    }
}

fn text_value(value: &SqlValue) -> String {
    match value {
        SqlValue::Null => String::new(),
        SqlValue::Integer(v) => v.to_string(),
        SqlValue::Real(v) => v.to_string(),
        SqlValue::Text(v) => v.clone(),
        SqlValue::Blob(v) => {
            use std::fmt::Write;
            let mut out = String::from("X'");
            for b in v {
                let _ = write!(out, "{b:02x}");
            }
            out.push('\'');
            out
        }
    }
}
fn json_value(value: &SqlValue) -> serde_json::Value {
    use base64::Engine;
    match value {
        SqlValue::Null => serde_json::Value::Null,
        SqlValue::Integer(v) if v.unsigned_abs() > (1 << 53) => v.to_string().into(),
        SqlValue::Integer(v) => (*v).into(),
        SqlValue::Real(v) => {
            serde_json::Number::from_f64(*v).map_or(serde_json::Value::Null, Into::into)
        }
        SqlValue::Text(v) => v.clone().into(),
        SqlValue::Blob(v) => base64::engine::general_purpose::STANDARD.encode(v).into(),
    }
}
fn unique_columns(columns: &[String]) -> Vec<String> {
    let mut used = std::collections::BTreeSet::new();
    columns
        .iter()
        .map(|name| {
            let mut candidate = name.clone();
            let mut i = 2;
            while !used.insert(candidate.clone()) {
                candidate = format!("{name}_{i}");
                i += 1;
            }
            candidate
        })
        .collect()
}
fn render(rows: &Rows, format: Format, out: &mut impl Write) -> anyhow::Result<()> {
    let columns = unique_columns(&rows.columns);
    if matches!(format, Format::Json | Format::Jsonl) {
        if matches!(format, Format::Json) {
            write!(out, "[")?;
        }
        for (index, row) in rows.values.iter().enumerate() {
            if index > 0 && matches!(format, Format::Json) {
                write!(out, ",")?;
            }
            let object: serde_json::Map<_, _> = columns
                .iter()
                .cloned()
                .zip(row.iter().map(json_value))
                .collect();
            serde_json::to_writer(&mut *out, &object)?;
            if matches!(format, Format::Jsonl) {
                writeln!(out)?;
            }
        }
        if matches!(format, Format::Json) {
            writeln!(out, "]")?;
        }
        return Ok(());
    }
    let escape = |s: &str| match format {
        Format::Csv if s.contains([',', '"', '\n', '\r']) => {
            format!("\"{}\"", s.replace('"', "\"\""))
        }
        Format::Tsv => s
            .replace('\\', "\\\\")
            .replace('\t', "\\t")
            .replace('\n', "\\n")
            .replace('\r', "\\r"),
        Format::Table => s
            .chars()
            .flat_map(|c| {
                if c.is_control() {
                    c.escape_default().collect::<Vec<_>>()
                } else {
                    vec![c]
                }
            })
            .take(80)
            .collect(),
        _ => s.to_owned(),
    };
    let lines: Vec<Vec<String>> = std::iter::once(columns)
        .chain(
            rows.values
                .iter()
                .map(|r| r.iter().map(text_value).collect()),
        )
        .map(|r| r.iter().map(|v| escape(v)).collect())
        .collect();
    let widths: Vec<usize> = (0..rows.columns.len())
        .map(|i| {
            lines
                .iter()
                .map(|r| r[i].chars().count())
                .max()
                .unwrap_or(0)
        })
        .collect();
    for row in lines {
        for (i, cell) in row.iter().enumerate() {
            if i > 0 {
                write!(
                    out,
                    "{}",
                    match format {
                        Format::Csv => ",",
                        Format::Tsv => "\t",
                        _ => " | ",
                    }
                )?;
            }
            if matches!(format, Format::Table) {
                write!(out, "{cell:width$}", width = widths[i])?;
            } else {
                write!(out, "{cell}")?;
            }
        }
        write!(
            out,
            "{}",
            if matches!(format, Format::Csv) {
                "\r\n"
            } else {
                "\n"
            }
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    #[test]
    fn signal_cleanup_child() {
        let Some(parent) = std::env::var_os("WASMKEEP_SIGNAL_TEST_DIR") else {
            return;
        };
        let result = run(async {
            let scratch = tempfile::tempdir_in(&parent)?;
            std::fs::write(scratch.path().join("plaintext"), b"private")?;
            std::fs::write(Path::new(&parent).join("ready"), b"")?;
            std::future::pending::<anyhow::Result<()>>().await
        });
        assert!(result.is_err());
        assert_eq!(std::fs::read_dir(parent).unwrap().count(), 1);
    }

    #[cfg(unix)]
    #[test]
    fn interrupt_and_termination_remove_scratch() {
        for signal in [rustix::process::Signal::INT, rustix::process::Signal::TERM] {
            let dir = tempfile::tempdir().unwrap();
            let mut child = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "actor_read::tests::signal_cleanup_child"])
                .env("WASMKEEP_SIGNAL_TEST_DIR", dir.path())
                .stdout(std::process::Stdio::null())
                .spawn()
                .unwrap();
            let start = std::time::Instant::now();
            while !dir.path().join("ready").exists() {
                if start.elapsed() > std::time::Duration::from_secs(10) {
                    let _ = child.kill();
                    let _ = child.wait();
                    panic!("signal test child did not become ready");
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            rustix::process::kill_process(
                rustix::process::Pid::from_raw(child.id() as i32).unwrap(),
                signal,
            )
            .unwrap();
            loop {
                if let Some(status) = child.try_wait().unwrap() {
                    assert!(status.success());
                    break;
                }
                if start.elapsed() > std::time::Duration::from_secs(10) {
                    let _ = child.kill();
                    let _ = child.wait();
                    panic!("signal cleanup did not finish");
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
        }
    }

    #[test]
    fn owner_mismatch_has_a_distinct_exit_code() {
        assert_eq!(
            failure_code(&anyhow::Error::new(history::Error::OwnerMismatch)),
            3
        );
        assert_eq!(failure_code(&anyhow::anyhow!("ordinary error")), 1);
    }
    #[test]
    fn output_formats_escape_values_and_preserve_types() {
        let rows = Rows {
            columns: ["v", "v", "blob", "null", "large"]
                .map(String::from)
                .to_vec(),
            values: vec![vec![
                SqlValue::Text("a,\"b\n\t\\".into()),
                SqlValue::Integer(4),
                SqlValue::Blob(vec![0, 255]),
                SqlValue::Null,
                SqlValue::Integer(i64::MAX),
            ]],
        };
        let rendered = |format| {
            let mut out = Vec::new();
            render(&rows, format, &mut out).unwrap();
            String::from_utf8(out).unwrap()
        };
        assert_eq!(
            rendered(Format::Csv),
            "v,v_2,blob,null,large\r\n\"a,\"\"b\n\t\\\",4,X'00ff',,9223372036854775807\r\n"
        );
        assert!(rendered(Format::Tsv).contains("a,\"b\\n\\t\\\\\t4\tX'00ff'\t\t"));
        let json: serde_json::Value = serde_json::from_str(&rendered(Format::Json)).unwrap();
        assert_eq!(json[0]["v_2"], 4);
        assert_eq!(json[0]["blob"], "AP8=");
        assert!(json[0]["null"].is_null());
        assert_eq!(json[0]["large"], i64::MAX.to_string());
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&rendered(Format::Jsonl)).unwrap(),
            json[0]
        );
        assert!(rendered(Format::Table).contains(" | "));
        assert_eq!(
            unique_columns(&["a".into(), "a".into(), "a_2".into()]),
            ["a", "a_2", "a_2_2"]
        );
    }
    #[test]
    fn timestamps_require_a_timezone_and_relative_units() {
        assert_eq!(parse_at("1970-01-01T00:00:01Z").unwrap(), 1000);
        assert_eq!(parse_at("1970-01-01T01:00:01+01:00").unwrap(), 1000);
        assert!(parse_at("-15m").is_ok());
        for invalid in [
            "-🌍",
            "yesterday",
            "2026-01-01",
            "-10x",
            "-9999999999999999999d",
        ] {
            assert!(parse_at(invalid).is_err());
        }
    }
}
