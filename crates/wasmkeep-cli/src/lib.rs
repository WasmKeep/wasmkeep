//! Shared CLI backup executor, also exercised by cluster integration tests.
#![forbid(unsafe_code)]

use anyhow::Context;
pub mod backup;

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
