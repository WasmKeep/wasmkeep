//! Shared CLI backup executor, also exercised by cluster integration tests;
//! and `cluster verify`, for the same reason.
#![forbid(unsafe_code)]

use anyhow::Context;
mod actor_groups;
pub mod backup;
pub mod blob_links;
pub mod feed;
pub mod markers;
pub mod verify;

/// Parses `TENANT/APP`.
pub fn parse_app(s: &str) -> Result<(String, String), String> {
    let (tenant, app) = s
        .split_once('/')
        .filter(|(t, a)| !t.is_empty() && !a.is_empty() && !a.contains('/'))
        .ok_or_else(|| format!("expected TENANT/APP, got {s:?}"))?;
    Ok((tenant.to_owned(), app.to_owned()))
}

/// Opens a bucket without migration checks, for status and archive commands.
pub fn open_bucket_unchecked(
    url: &str,
) -> anyhow::Result<(std::sync::Arc<dyn object_store::ObjectStore>, String)> {
    let (bucket, prefix) = parse_bucket(url)?;
    let store = object_store::aws::AmazonS3Builder::from_env()
        .with_bucket_name(bucket)
        .build()
        .with_context(|| format!("opening bucket {url}"))?;
    let store: std::sync::Arc<dyn object_store::ObjectStore> = std::sync::Arc::new(store);
    Ok((store, prefix.to_owned()))
}

/// Splits an S3 URL into bucket and prefix.
pub fn parse_bucket(url: &str) -> anyhow::Result<(&str, &str)> {
    let rest = url
        .strip_prefix("s3://")
        .with_context(|| format!("--bucket must look like s3://BUCKET[/PREFIX], got {url:?}"))?;
    let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
    anyhow::ensure!(!bucket.is_empty(), "--bucket has no bucket name: {url:?}");
    Ok((bucket, prefix.trim_matches('/')))
}

/// Stack for every thread of the serving runtime, its blocking pool
/// included (tokio's default is 2 MiB). SQLite runs on those threads, and
/// its own `LIKE` and `GLOB` recurse once per wildcard: a 44 KB pattern
/// overflowed 2 MiB and aborted the process. The host's versions do not
/// recurse; this is the margin for anything of SQLite's that still does.
pub const SERVING_THREAD_STACK: usize = 8 << 20;

/// The multi-threaded runtime `serve` and `dev` run on.
pub fn serving_runtime() -> std::io::Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(SERVING_THREAD_STACK)
        .build()
}

/// Runs a command with signal cancellation and background runtime shutdown.
pub fn run_async(
    work: impl std::future::Future<Output = anyhow::Result<()>>,
) -> anyhow::Result<()> {
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

/// Creates a server's data directory for this account alone if it is
/// missing, and warns when an existing one is open to other accounts.
pub fn prepare_data_root(dir: &std::path::Path) -> anyhow::Result<()> {
    match wasmkeep_storage::prepare_data_root(dir)
        .with_context(|| format!("preparing the data directory {}", dir.display()))?
    {
        wasmkeep_storage::DataRoot::Loose(mode) => tracing::warn!(
            data_dir = %dir.display(),
            mode = format!("{mode:o}"),
            "the data directory is readable by other accounts; consider chmod 700"
        ),
        wasmkeep_storage::DataRoot::Created | wasmkeep_storage::DataRoot::Private => {}
    }
    Ok(())
}

/// A scratch directory only this account can open (0700 on Unix), for
/// copies of actor data: `tempfile::tempdir` leaves the mode to the umask.
pub fn private_tempdir() -> std::io::Result<tempfile::TempDir> {
    private_builder().tempdir()
}

/// [`private_tempdir`] under `parent`.
pub fn private_tempdir_in(
    parent: impl AsRef<std::path::Path>,
) -> std::io::Result<tempfile::TempDir> {
    private_builder().tempdir_in(parent)
}

fn private_builder() -> tempfile::Builder<'static, 'static> {
    #[allow(unused_mut)] // only Unix sets a mode
    let mut builder = tempfile::Builder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        builder.permissions(std::fs::Permissions::from_mode(0o700));
    }
    builder
}

#[cfg(all(test, unix))]
mod private_tempdir_tests {
    #![allow(clippy::unwrap_used)] // test code
    use std::os::unix::fs::PermissionsExt;

    fn mode(dir: &tempfile::TempDir) -> u32 {
        std::fs::metadata(dir.path()).unwrap().permissions().mode() & 0o777
    }

    #[test]
    fn scratch_directories_are_owner_only() {
        let parent = tempfile::tempdir().unwrap();
        assert_eq!(mode(&super::private_tempdir().unwrap()), 0o700);
        assert_eq!(
            mode(&super::private_tempdir_in(parent.path()).unwrap()),
            0o700
        );
    }
}
