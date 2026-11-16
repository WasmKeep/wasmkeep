//! `wasmkeep app format`: an app's ownership-format setting ([ADR 0035],
//! section 5), as the nodes read it. Read only: this version offers no
//! way to enable the recorded-head format, which its servers read but do
//! not enforce yet. `cluster diagnose` probes the bucket's semantics, not
//! this setting; its result says nothing about either.
//!
//! [ADR 0035]: ../../../docs/adr/0035-a-recorded-head-on-hand-over.md

use object_store::ObjectStoreExt as _;
use wasmkeep_cluster::app_format::{self, AppFormat};

use crate::AppFormatArgs;

/// `wasmkeep app format`.
pub(crate) fn run(args: &AppFormatArgs) -> anyhow::Result<()> {
    let (store, prefix) = crate::open_bucket(&args.bucket)?;
    let prefix = if prefix.is_empty() {
        prefix
    } else {
        format!("{prefix}/")
    };
    let (tenant, app) = &args.app;
    let key = app_format::key(&prefix, tenant, app);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let body = rt.block_on(async {
        match store
            .get(&object_store::path::Path::from(key.as_str()))
            .await
        {
            Ok(got) => got.bytes().await.map(|b| Some(b.to_vec())),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e),
        }
    })?;
    println!("{}", describe(tenant, app, &key, body.as_deref()));
    Ok(())
}

/// What the command prints for the setting at `key` (`None`: absent).
pub(crate) fn describe(tenant: &str, app: &str, key: &str, body: Option<&[u8]>) -> String {
    let setting = match app_format::decode(body) {
        Ok(AppFormat::Version1) => {
            format!("ownership records in version 1 (wasmkeep-owner 1): no setting at {key}")
        }
        Ok(AppFormat::RecordedHead) => format!(
            "the recorded-head format is enabled (wasmkeep-owner 2), at {key}: servers of \
             this version refuse to serve the app"
        ),
        Err(e) => format!("{e}: servers refuse to serve the app (at {key})"),
    };
    format!(
        "{tenant}/{app}: {setting}\n\
         This CLI and servers of this version read ownership records of both versions \
         and write version 1; none advertises the `recorded-head` capability, and none \
         can enable the format. `cluster diagnose` does not check this setting."
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_setting_is_named() {
        let key = "formats/t/a/recorded-head";
        assert!(describe("t", "a", key, None).contains("version 1"));
        assert!(describe("t", "a", key, Some(app_format::ENABLED_BODY)).contains("is enabled"));
        let unknown = describe("t", "a", key, Some(b"junk"));
        assert!(
            unknown.contains("unreadable") && unknown.contains("refuse"),
            "{unknown}"
        );
    }
}
