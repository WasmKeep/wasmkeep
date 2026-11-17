//! The host bindings and the guest SDK agree: examples/counter, built with
//! wasmkeep-sdk, instantiates against `actor-core` host bindings (with fake
//! storage) plus WASI, and its `on-init` reaches the host with the right SQL.
//! Components that use `blobs` link in both worlds, and those that do not
//! import it.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::path::PathBuf;
use std::process::Command;

use wasmkeep_runtime::{Runtime, RuntimeConfig};
use wasmkeep_wit::ActorCorePre;
use wasmkeep_wit::wasmkeep::actor::actors::{CallError, Request, Response};
use wasmkeep_wit::wasmkeep::actor::blobs::{self, BlobError, BlobInfo, Listing, Upload};
use wasmkeep_wit::wasmkeep::actor::types::{Rows, SqlError, Value};
use wasmkeep_wit::wasmkeep::actor::{actors, alarms, calls, env, storage, types, websockets};
use wasmtime::component::{HasSelf, Linker, Resource, ResourceTable};
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};
use wasmtime_wasi_http::{WasiHttpCtx, WasiHttpCtxView, WasiHttpView};

fn build(dir: &str, name: &str) -> Vec<u8> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(dir);
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".into());
    let status = Command::new(cargo)
        .current_dir(&root)
        .args([
            "build",
            "-j",
            "4",
            "--release",
            "--locked",
            "--target",
            "wasm32-wasip2",
        ])
        .status()
        .unwrap();
    assert!(status.success(), "building {dir} failed");
    std::fs::read(root.join(format!("target/wasm32-wasip2/release/{name}.wasm"))).unwrap()
}

/// Links `actor-core` interface by interface: the world's `add_to_linker`
/// would define `wasi:io` again, which `blobs` uses and WASI links.
fn link_actor_core(linker: &mut Linker<State>) {
    fn get(s: &mut State) -> &mut Fake {
        &mut s.fake
    }
    types::add_to_linker::<State, HasSelf<Fake>>(linker, get).unwrap();
    storage::add_to_linker::<State, HasSelf<Fake>>(linker, get).unwrap();
    alarms::add_to_linker::<State, HasSelf<Fake>>(linker, get).unwrap();
    actors::add_to_linker::<State, HasSelf<Fake>>(linker, get).unwrap();
    calls::add_to_linker::<State, HasSelf<Fake>>(linker, get).unwrap();
    websockets::add_to_linker::<State, HasSelf<Fake>>(linker, get).unwrap();
    env::add_to_linker::<State, HasSelf<Fake>>(linker, get).unwrap();
    blobs::add_to_linker::<State, HasSelf<Fake>>(linker, get).unwrap();
}

fn linker(runtime: &Runtime) -> Linker<State> {
    let mut linker = Linker::<State>::new(runtime.engine());
    wasmtime_wasi::p2::add_to_linker_async(&mut linker).unwrap();
    wasmtime_wasi_http::p2::add_only_http_to_linker_async(&mut linker).unwrap();
    link_actor_core(&mut linker);
    linker
}

fn counter_wasm() -> Vec<u8> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/counter");
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".into());
    let status = Command::new(cargo)
        .current_dir(&root)
        .args([
            "build",
            "--release",
            "--locked",
            "--target",
            "wasm32-wasip2",
        ])
        .status()
        .unwrap();
    assert!(status.success(), "building examples/counter failed");
    std::fs::read(root.join("target/wasm32-wasip2/release/counter.wasm")).unwrap()
}

/// Records every storage call; alarms kept in a field.
#[derive(Default)]
struct Fake {
    statements: Vec<(String, Vec<Value>)>,
    alarm: Option<u64>,
}

struct State {
    fake: Fake,
    wasi: WasiCtx,
    http: WasiHttpCtx,
    table: ResourceTable,
}

impl WasiView for State {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi,
            table: &mut self.table,
        }
    }
}

impl WasiHttpView for State {
    fn http(&mut self) -> WasiHttpCtxView<'_> {
        WasiHttpCtxView {
            ctx: &mut self.http,
            table: &mut self.table,
            hooks: Default::default(),
        }
    }
}

impl types::Host for Fake {}

impl storage::Host for Fake {
    async fn exec(
        &mut self,
        sql: String,
        params: Vec<Value>,
    ) -> wasmtime::Result<Result<u64, SqlError>> {
        self.statements.push((sql, params));
        Ok(Ok(1))
    }

    async fn exec_durable(
        &mut self,
        sql: String,
        params: Vec<Value>,
    ) -> wasmtime::Result<Result<u64, SqlError>> {
        self.exec(sql, params).await
    }

    async fn query(
        &mut self,
        sql: String,
        params: Vec<Value>,
    ) -> wasmtime::Result<Result<Rows, SqlError>> {
        self.statements.push((sql, params));
        Ok(Ok(Rows {
            columns: vec![],
            values: vec![],
        }))
    }

    async fn delete_self(&mut self) -> wasmtime::Result<()> {
        Ok(())
    }

    async fn delete_self_durable(&mut self) -> wasmtime::Result<Result<(), SqlError>> {
        Ok(Ok(()))
    }

    async fn flush(&mut self) -> wasmtime::Result<Result<(), SqlError>> {
        Ok(Ok(()))
    }
}

impl alarms::Host for Fake {
    async fn set_alarm(&mut self, at: u64) -> wasmtime::Result<()> {
        self.alarm = Some(at);
        Ok(())
    }

    async fn get_alarm(&mut self) -> wasmtime::Result<Option<u64>> {
        Ok(self.alarm)
    }

    async fn clear_alarm(&mut self) -> wasmtime::Result<()> {
        self.alarm = None;
        Ok(())
    }
}

impl websockets::Host for Fake {
    async fn send(
        &mut self,
        _conn: u64,
        _msg: websockets::Message,
    ) -> wasmtime::Result<Result<(), websockets::SendError>> {
        Ok(Err(websockets::SendError::UnknownConnection))
    }

    async fn close(&mut self, _conn: u64, _code: u16, _reason: String) -> wasmtime::Result<()> {
        Ok(())
    }

    async fn connections(&mut self) -> wasmtime::Result<Vec<u64>> {
        Ok(Vec::new())
    }
}

fn no_files() -> BlobError {
    BlobError::Unavailable("fake".into())
}

impl blobs::HostUpload for Fake {
    async fn body(
        &mut self,
        _upload: Resource<Upload>,
    ) -> wasmtime::Result<Result<Resource<wasmtime_wasi::p2::DynOutputStream>, BlobError>> {
        Ok(Err(no_files()))
    }

    async fn finish(
        &mut self,
        _upload: Resource<Upload>,
    ) -> wasmtime::Result<Result<BlobInfo, BlobError>> {
        Ok(Err(no_files()))
    }

    async fn drop(&mut self, _upload: Resource<Upload>) -> wasmtime::Result<()> {
        Ok(())
    }
}

impl blobs::Host for Fake {
    async fn head(
        &mut self,
        _key: String,
    ) -> wasmtime::Result<Result<Option<BlobInfo>, BlobError>> {
        Ok(Err(no_files()))
    }

    async fn get(
        &mut self,
        _key: String,
    ) -> wasmtime::Result<Result<Option<(BlobInfo, Vec<u8>)>, BlobError>> {
        Ok(Err(no_files()))
    }

    async fn put(
        &mut self,
        _key: String,
        _body: Vec<u8>,
        _options: blobs::WriteOptions,
    ) -> wasmtime::Result<Result<BlobInfo, BlobError>> {
        Ok(Err(no_files()))
    }

    async fn open_read(
        &mut self,
        _key: String,
        _range: Option<(u64, u64)>,
    ) -> wasmtime::Result<
        Result<Option<(BlobInfo, Resource<wasmtime_wasi::p2::DynInputStream>)>, BlobError>,
    > {
        Ok(Err(no_files()))
    }

    async fn open_write(
        &mut self,
        _key: String,
        _options: blobs::WriteOptions,
    ) -> wasmtime::Result<Result<Resource<Upload>, BlobError>> {
        Ok(Err(no_files()))
    }

    async fn delete(&mut self, _key: String) -> wasmtime::Result<Result<(), BlobError>> {
        Ok(Err(no_files()))
    }

    async fn list(
        &mut self,
        _prefix: String,
        _after: Option<String>,
        _limit: u32,
    ) -> wasmtime::Result<Result<Listing, BlobError>> {
        Ok(Err(no_files()))
    }

    async fn link_download(
        &mut self,
        _key: String,
        _options: blobs::DownloadOptions,
    ) -> wasmtime::Result<Result<String, BlobError>> {
        Ok(Err(no_files()))
    }

    async fn link_upload(
        &mut self,
        _key: String,
        _options: blobs::UploadOptions,
    ) -> wasmtime::Result<Result<blobs::UploadLink, BlobError>> {
        Ok(Err(no_files()))
    }
}

impl env::Host for Fake {
    async fn get(&mut self, _name: String) -> wasmtime::Result<Option<String>> {
        Ok(None)
    }
}

impl actors::Host for Fake {
    async fn call(
        &mut self,
        _address: String,
        _request: Request,
    ) -> wasmtime::Result<Result<Response, CallError>> {
        Ok(Err(CallError::Reentrant))
    }

    async fn self_address(&mut self) -> wasmtime::Result<actors::ActorAddress> {
        Ok(actors::ActorAddress {
            tenant: "default".into(),
            class: "counter".into(),
            name: "fake".into(),
        })
    }
}

impl calls::Host for Fake {
    async fn call_many(
        &mut self,
        calls: Vec<(String, Request)>,
    ) -> wasmtime::Result<Vec<Result<Response, CallError>>> {
        Ok(calls.iter().map(|_| Err(CallError::Reentrant)).collect())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn counter_on_init_reaches_the_host() {
    let runtime = Runtime::new(&RuntimeConfig {
        max_instances: 4,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let component = runtime.compile(&counter_wasm()).unwrap();
    // The counter does not use `calls` (ADR 0025): its component does not
    // import it, as one built before the interface existed, and links
    // against host bindings that provide it.
    let imports: Vec<String> = component
        .component_type()
        .imports(runtime.engine())
        .map(|(name, _)| name.to_owned())
        .collect();
    assert!(
        !imports
            .iter()
            .any(|i| i.starts_with("wasmkeep:actor/calls@")),
        "{imports:?}"
    );
    // Nor `blobs` (ADR 0030): a component that does not use files imports
    // none, and links on a node with or without them.
    assert!(
        !imports
            .iter()
            .any(|i| i.starts_with("wasmkeep:actor/blobs@")),
        "{imports:?}"
    );

    let linker = linker(&runtime);

    let pre = ActorCorePre::new(runtime.instantiate_pre(&linker, &component).unwrap()).unwrap();
    let mut store = runtime.new_async_store(State {
        fake: Fake::default(),
        wasi: WasiCtx::builder().build(),
        http: WasiHttpCtx::new(),
        table: ResourceTable::new(),
    });
    let actor = pre.instantiate_async(&mut store).await.unwrap();

    actor.call_on_init(&mut store).await.unwrap().unwrap();
    actor.call_on_alarm(&mut store).await.unwrap().unwrap();

    let statements = &store.data().fake.statements;
    assert_eq!(statements.len(), 2, "{statements:?}");
    assert!(
        statements[0]
            .0
            .contains("CREATE TABLE IF NOT EXISTS counter")
    );
    assert!(statements[0].1.is_empty());
    assert_eq!(
        statements[1].0,
        "INSERT OR IGNORE INTO counter (id, value) VALUES (?1, ?2)"
    );
    assert!(matches!(
        statements[1].1.as_slice(),
        [Value::Integer(1), Value::Integer(0)]
    ));
}

#[test]
fn hello_is_not_an_actor() {
    // A plain wasi:http component lacks the actor exports.
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/hello");
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".into());
    let status = Command::new(cargo)
        .current_dir(&root)
        .args([
            "build",
            "--release",
            "--locked",
            "--target",
            "wasm32-wasip2",
        ])
        .status()
        .unwrap();
    assert!(status.success());
    let wasm = std::fs::read(root.join("target/wasm32-wasip2/release/hello.wasm")).unwrap();

    let runtime = Runtime::new(&RuntimeConfig {
        max_instances: 2,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let component = runtime.compile(&wasm).unwrap();
    let linker = linker(&runtime);
    let pre = runtime.instantiate_pre(&linker, &component).unwrap();
    assert!(ActorCorePre::new(pre).is_err());
}

/// A component that uses files imports `blobs` and `wasi:io/streams`, and
/// links: an actor against `actor-core`, a front against `front-core`.
#[test]
fn components_using_files_link_in_both_worlds() {
    let runtime = Runtime::new(&RuntimeConfig {
        max_instances: 2,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let blobs_import = |c: &wasmtime::component::Component| {
        c.component_type()
            .imports(runtime.engine())
            .any(|(name, _)| name == "wasmkeep:actor/blobs@0.2.0")
    };
    let actor = runtime
        .compile(&build(
            "../wasmkeep-server/tests/guests/blob-actor",
            "blob_actor",
        ))
        .unwrap();
    assert!(blobs_import(&actor));
    let pre = runtime.instantiate_pre(&linker(&runtime), &actor).unwrap();
    ActorCorePre::new(pre).unwrap();

    let front = runtime
        .compile(&build(
            "../wasmkeep-server/tests/guests/test-front",
            "test_front",
        ))
        .unwrap();
    assert!(blobs_import(&front));
    let mut linker = Linker::<State>::new(runtime.engine());
    wasmtime_wasi::p2::add_to_linker_async(&mut linker).unwrap();
    wasmtime_wasi_http::p2::add_only_http_to_linker_async(&mut linker).unwrap();
    fn get(s: &mut State) -> &mut Fake {
        &mut s.fake
    }
    actors::add_to_linker::<State, HasSelf<Fake>>(&mut linker, get).unwrap();
    calls::add_to_linker::<State, HasSelf<Fake>>(&mut linker, get).unwrap();
    env::add_to_linker::<State, HasSelf<Fake>>(&mut linker, get).unwrap();
    blobs::add_to_linker::<State, HasSelf<Fake>>(&mut linker, get).unwrap();
    let pre = runtime.instantiate_pre(&linker, &front).unwrap();
    wasmtime_wasi_http::p2::bindings::ProxyPre::new(pre).unwrap();
}
