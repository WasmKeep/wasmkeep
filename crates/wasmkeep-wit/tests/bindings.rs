//! The host bindings and the guest SDK agree: examples/counter, built with
//! wasmkeep-sdk, instantiates against `actor-core` host bindings (with fake
//! storage) plus WASI, and its `on-init` reaches the host with the right SQL.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::path::PathBuf;
use std::process::Command;

use wasmkeep_runtime::{Runtime, RuntimeConfig};
use wasmkeep_wit::wasmkeep::actor::actors::{CallError, Request, Response};
use wasmkeep_wit::wasmkeep::actor::types::{Rows, SqlError, Value};
use wasmkeep_wit::wasmkeep::actor::{actors, alarms, calls, storage, types, websockets};
use wasmkeep_wit::{ActorCore, ActorCorePre};
use wasmtime::component::{HasSelf, Linker, ResourceTable};
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};
use wasmtime_wasi_http::{WasiHttpCtx, WasiHttpCtxView, WasiHttpView};

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

impl wasmkeep_wit::wasmkeep::actor::env::Host for Fake {
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

    let mut linker = Linker::<State>::new(runtime.engine());
    wasmtime_wasi::p2::add_to_linker_async(&mut linker).unwrap();
    wasmtime_wasi_http::p2::add_only_http_to_linker_async(&mut linker).unwrap();
    ActorCore::add_to_linker::<State, HasSelf<Fake>>(&mut linker, |s| &mut s.fake).unwrap();

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
    let mut linker = Linker::<State>::new(runtime.engine());
    wasmtime_wasi::p2::add_to_linker_async(&mut linker).unwrap();
    wasmtime_wasi_http::p2::add_only_http_to_linker_async(&mut linker).unwrap();
    ActorCore::add_to_linker::<State, HasSelf<Fake>>(&mut linker, |s| &mut s.fake).unwrap();
    let pre = runtime.instantiate_pre(&linker, &component).unwrap();
    assert!(ActorCorePre::new(pre).is_err());
}
