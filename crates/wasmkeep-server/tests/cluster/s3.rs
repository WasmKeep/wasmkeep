//! Bucket-gated variants use an isolated prefix through a real S3 API.
use super::*;
use futures_util::{FutureExt, TryStreamExt};
use object_store::ObjectStoreExt;

pub(super) async fn run(name: &str, test: impl AsyncFnOnce(Arc<dyn ObjectStore>)) {
    let Ok(bucket) = std::env::var("WASMKEEP_TEST_S3_BUCKET") else {
        eprintln!("WASMKEEP_TEST_S3_BUCKET not set; skipping S3 {name}");
        return;
    };
    let s3 = object_store::aws::AmazonS3Builder::from_env()
        .with_bucket_name(bucket)
        .build()
        .unwrap();
    let prefix = format!(
        "testprep-{name}-{}",
        wasmkeep_cluster::control::new_request_id()
    );
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::prefix::PrefixStore::new(s3, prefix));
    let result = std::panic::AssertUnwindSafe(test(store.clone()))
        .catch_unwind()
        .await;
    for meta in store.list(None).try_collect::<Vec<_>>().await.unwrap() {
        store.delete(&meta.location).await.unwrap();
    }
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}
