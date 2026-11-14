//! Generates the API reference (`docs/reference/api.md`) from the WIT
//! package, and fails when the checked-in page is out of date.
//!
//! Regenerate with:
//!
//! ```text
//! WASMKEEP_BLESS=1 cargo nextest run -p wasmkeep-wit --test api_reference
//! ```

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::path::PathBuf;

use wit_parser::{
    Docs, Function, Interface, Resolve, Type, TypeDefKind, TypeId, WorldItem, WorldKey,
};

/// Each WIT function's name in every SDK: Rust, TypeScript, Python, C, Zig.
/// A new function in the WIT fails `every_function_has_sdk_names` until it is
/// listed here (and implemented in the SDKs). An empty name: that SDK does
/// not have it yet.
const SDK_NAMES: &[(&str, [&str; 5])] = &[
    // Exports: what an actor implements.
    (
        "on-init",
        [
            "Actor::on_init",
            "init()",
            "Actor.init",
            "wk_init",
            "pub fn init",
        ],
    ),
    (
        "on-alarm",
        [
            "Actor::on_alarm",
            "alarm()",
            "Actor.alarm",
            "wk_alarm",
            "pub fn alarm",
        ],
    ),
    (
        "on-websocket-open",
        [
            "Actor::on_websocket_open",
            "websocket.open()",
            "Actor.websocket_open",
            "wk_ws_open",
            "pub fn wsOpen",
        ],
    ),
    (
        "on-websocket-message",
        [
            "Actor::on_websocket_message",
            "websocket.message()",
            "Actor.websocket_message",
            "wk_ws_message",
            "pub fn wsMessage",
        ],
    ),
    (
        "on-websocket-close",
        [
            "Actor::on_websocket_close",
            "websocket.close()",
            "Actor.websocket_close",
            "wk_ws_close",
            "pub fn wsClose",
        ],
    ),
    // Imports: what an actor can call.
    (
        "storage.exec",
        [
            "storage::exec",
            "storage.exec",
            "storage.exec",
            "wk_exec",
            "wk.exec",
        ],
    ),
    (
        "storage.exec-durable",
        [
            "storage::exec_durable",
            "storage.execDurable",
            "storage.exec_durable",
            "wk_exec_durable",
            "wk.execDurable",
        ],
    ),
    (
        "storage.query",
        [
            "storage::query",
            "storage.query",
            "storage.query",
            "wk_query",
            "wk.query",
        ],
    ),
    (
        "storage.delete-self",
        [
            "storage::delete_self",
            "storage.deleteSelf",
            "storage.delete_self",
            "wk_delete_self",
            "wk.deleteSelf",
        ],
    ),
    (
        "alarms.set-alarm",
        [
            "alarms::set_alarm",
            "alarms.set",
            "alarms.set",
            "wk_alarm_set",
            "wk.setAlarm",
        ],
    ),
    (
        "alarms.get-alarm",
        [
            "alarms::get_alarm",
            "alarms.get",
            "alarms.get",
            "wk_alarm_get",
            "wk.getAlarm",
        ],
    ),
    (
        "alarms.clear-alarm",
        [
            "alarms::clear_alarm",
            "alarms.clear",
            "alarms.clear",
            "wk_alarm_clear",
            "wk.clearAlarm",
        ],
    ),
    (
        "actors.call",
        [
            "actors::call",
            "actors.call",
            "actors.call",
            "wk_call",
            "wk.call",
        ],
    ),
    (
        "actors.self-address",
        [
            "actors::self_address",
            "actors.self()",
            "actors.self_address",
            "wk_self_address",
            "wk.selfAddress",
        ],
    ),
    // Adopted SDK by SDK (ADR 0025): "" is "not yet".
    (
        "calls.call-many",
        ["calls::call_many", "actors.callMany", "", "", ""],
    ),
    (
        "env.get",
        ["env::get", "env.get", "env.get", "wk_env", "wk.env"],
    ),
    (
        "websockets.send",
        [
            "websockets::send",
            "sockets.send",
            "sockets.send",
            "wk_send, wk_send_binary",
            "wk.send, wk.sendBinary",
        ],
    ),
    (
        "websockets.close",
        [
            "websockets::close",
            "sockets.close",
            "sockets.close",
            "wk_close",
            "wk.close",
        ],
    ),
    (
        "websockets.connections",
        [
            "websockets::connections",
            "sockets.connections",
            "sockets.connections",
            "wk_connections",
            "wk.connections",
        ],
    ),
];

const LANGUAGES: [&str; 5] = ["Rust", "TypeScript", "Python", "C", "Zig"];

fn page_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../docs/reference/api.md")
}

fn load() -> (Resolve, wit_parser::PackageId) {
    let mut resolve = Resolve::default();
    let (package, _) = resolve.push_dir(wasmkeep_wit::WIT_DIR).unwrap();
    (resolve, package)
}

/// WIT syntax for a type.
fn ty(resolve: &Resolve, t: &Type) -> String {
    match t {
        Type::Bool => "bool".into(),
        Type::U8 => "u8".into(),
        Type::U16 => "u16".into(),
        Type::U32 => "u32".into(),
        Type::U64 => "u64".into(),
        Type::S8 => "s8".into(),
        Type::S16 => "s16".into(),
        Type::S32 => "s32".into(),
        Type::S64 => "s64".into(),
        Type::F32 => "f32".into(),
        Type::F64 => "f64".into(),
        Type::Char => "char".into(),
        Type::String => "string".into(),
        Type::ErrorContext => "error-context".into(),
        Type::Id(id) => type_id(resolve, *id),
    }
}

fn type_id(resolve: &Resolve, id: TypeId) -> String {
    let def = &resolve.types[id];
    if let Some(name) = &def.name {
        return name.clone();
    }
    match &def.kind {
        TypeDefKind::List(t) => format!("list<{}>", ty(resolve, t)),
        TypeDefKind::Option(t) => format!("option<{}>", ty(resolve, t)),
        TypeDefKind::Tuple(t) => format!(
            "tuple<{}>",
            t.types
                .iter()
                .map(|t| ty(resolve, t))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        TypeDefKind::Result(r) => {
            let part = |t: &Option<Type>| t.as_ref().map_or("_".to_string(), |t| ty(resolve, t));
            match (&r.ok, &r.err) {
                (None, None) => "result".into(),
                (ok, None) => format!("result<{}>", part(ok)),
                (ok, err) => format!("result<{}, {}>", part(ok), part(err)),
            }
        }
        TypeDefKind::Type(t) => ty(resolve, t),
        other => panic!("type not supported by the reference generator: {other:?}"),
    }
}

fn signature(resolve: &Resolve, f: &Function) -> String {
    let params = f
        .params
        .iter()
        .map(|p| format!("{}: {}", p.name, ty(resolve, &p.ty)))
        .collect::<Vec<_>>()
        .join(", ");
    let result = f
        .result
        .as_ref()
        .map_or(String::new(), |r| format!(" -> {}", ty(resolve, r)));
    format!("{}: func({params}){result};", f.name)
}

fn docs(out: &mut String, d: &Docs) {
    if let Some(text) = &d.contents {
        out.push_str(text.trim());
        out.push_str("\n\n");
    }
}

/// One line of a doc comment, for table cells.
fn inline(d: &Docs) -> String {
    d.contents
        .as_deref()
        .unwrap_or("")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn sdk_table(out: &mut String, key: &str) {
    let names = SDK_NAMES
        .iter()
        .find(|(k, _)| *k == key)
        .map_or_else(|| panic!("no SDK names for {key}"), |(_, n)| n);
    out.push_str("| ");
    out.push_str(&LANGUAGES.join(" | "));
    out.push_str(" |\n|");
    out.push_str(&" --- |".repeat(LANGUAGES.len()));
    out.push_str("\n|");
    for name in names {
        if name.is_empty() {
            out.push_str(" not yet |");
        } else {
            write!(out, " `{name}` |").unwrap();
        }
    }
    out.push_str("\n\n");
}

fn function(out: &mut String, resolve: &Resolve, f: &Function, key: &str) {
    writeln!(out, "### `{}`\n", f.name).unwrap();
    writeln!(out, "```wit\n{}\n```\n", signature(resolve, f)).unwrap();
    docs(out, &f.docs);
    sdk_table(out, key);
}

fn type_def(out: &mut String, resolve: &Resolve, name: &str, id: TypeId) {
    let def = &resolve.types[id];
    let (kind, rows): (&str, Vec<(String, String)>) = match &def.kind {
        TypeDefKind::Record(r) => (
            "record",
            r.fields
                .iter()
                .map(|f| {
                    (
                        format!("`{}: {}`", f.name, ty(resolve, &f.ty)),
                        inline(&f.docs),
                    )
                })
                .collect(),
        ),
        TypeDefKind::Variant(v) => (
            "variant",
            v.cases
                .iter()
                .map(|c| {
                    let case = match &c.ty {
                        Some(t) => format!("`{}({})`", c.name, ty(resolve, t)),
                        None => format!("`{}`", c.name),
                    };
                    (case, inline(&c.docs))
                })
                .collect(),
        ),
        TypeDefKind::Enum(e) => (
            "enum",
            e.cases
                .iter()
                .map(|c| (format!("`{}`", c.name), inline(&c.docs)))
                .collect(),
        ),
        // `use`d from another interface: documented there.
        TypeDefKind::Type(_) => return,
        other => panic!("type not supported by the reference generator: {other:?}"),
    };
    writeln!(out, "### `{name}` ({kind})\n").unwrap();
    docs(out, &def.docs);
    let header = if kind == "record" { "Field" } else { "Case" };
    writeln!(out, "| {header} | |\n| --- | --- |").unwrap();
    for (item, doc) in rows {
        writeln!(out, "| {item} | {doc} |").unwrap();
    }
    out.push('\n');
}

fn interface(out: &mut String, resolve: &Resolve, name: &str, iface: &Interface) {
    writeln!(out, "## Interface `{name}`\n").unwrap();
    docs(out, &iface.docs);
    for (type_name, id) in &iface.types {
        type_def(out, resolve, type_name, *id);
    }
    for f in iface.functions.values() {
        function(out, resolve, f, &format!("{name}.{}", f.name));
    }
}

fn generate() -> String {
    let (resolve, package_id) = load();
    let package = &resolve.packages[package_id];
    let world = &resolve.worlds[package.worlds["actor-core"]];
    let mut out = String::new();

    writeln!(
        out,
        "# API reference\n\n\
         <!-- Generated from crates/wasmkeep-wit/wit/actor.wit by \
         crates/wasmkeep-wit/tests/api_reference.rs. Do not edit: run \
         `WASMKEEP_BLESS=1 cargo nextest run -p wasmkeep-wit --test api_reference`. -->\n\n\
         `{}:{}@{}`, the interface between WasmKeep and every actor, in any \
         language. Each entry gives the WIT definition, then its name in each \
         SDK ([Rust](../../crates/wasmkeep-sdk), \
         [TypeScript](../../sdks/typescript), [Python](../../sdks/python), \
         [C](../../sdks/c), [Zig](../../sdks/zig)). The SDKs add conveniences \
         (typed rows, JSON, broadcast); see each SDK's README.\n",
        package.name.namespace,
        package.name.name,
        package.name.version.as_ref().unwrap(),
    )
    .unwrap();
    docs(&mut out, &package.docs);

    writeln!(
        out,
        "## What an actor implements\n\n\
         HTTP requests arrive through the standard `wasi:http/incoming-handler` \
         export (`handle`), which the TypeScript, Python, C and Zig SDKs expose \
         as their `fetch` handler and the Rust SDK as `http::Guest::handle`. \
         The world adds:\n"
    )
    .unwrap();
    for (key, item) in &world.exports {
        let (WorldKey::Name(_), WorldItem::Function(f)) = (key, item) else {
            continue;
        };
        function(&mut out, &resolve, f, &f.name);
    }

    for item in world.imports.values() {
        if let WorldItem::Interface { id, .. } = item {
            let iface = &resolve.interfaces[*id];
            let name = iface.name.as_deref().unwrap();
            if name != "types" {
                interface(&mut out, &resolve, name, iface);
            }
        }
    }
    // The shared types, last: referenced from the interfaces above.
    let types = &resolve.interfaces[package.interfaces["types"]];
    interface(&mut out, &resolve, "types", types);

    out.push_str(
        "## Also available\n\n\
         The `actor` world includes the standard `wasi:http/proxy` world: \
         outbound HTTP (`wasi:http/outgoing-handler`), clocks, randomness and \
         stdio (what an actor writes to stdout and stderr goes to the node's \
         log, line by line, within `--guest-log-rate`). The \
         `wasi:cli` interfaces most standard libraries import are present but \
         empty: no arguments, environment or terminal. `wasi:filesystem` and \
         `wasi:sockets`, which many toolchains import whether or not the code \
         uses them, are linked but reach nothing: no directory is opened to \
         the guest, and every socket and name lookup is refused. A component \
         importing an interface the host does not provide is refused at \
         deploy time.\n",
    );
    out
}

/// Every function in the world, keyed as in `SDK_NAMES`.
fn function_keys() -> BTreeSet<String> {
    let (resolve, package_id) = load();
    let package = &resolve.packages[package_id];
    let world = &resolve.worlds[package.worlds["actor-core"]];
    let mut keys = BTreeSet::new();
    for item in world.exports.values() {
        if let WorldItem::Function(f) = item {
            keys.insert(f.name.clone());
        }
    }
    for item in world.imports.values() {
        if let WorldItem::Interface { id, .. } = item {
            let iface = &resolve.interfaces[*id];
            for f in iface.functions.values() {
                keys.insert(format!("{}.{}", iface.name.as_deref().unwrap(), f.name));
            }
        }
    }
    keys
}

#[test]
fn every_function_has_sdk_names() {
    let wit = function_keys();
    let listed: BTreeSet<String> = SDK_NAMES.iter().map(|(k, _)| (*k).to_string()).collect();
    let missing: Vec<_> = wit.difference(&listed).collect();
    let stale: Vec<_> = listed.difference(&wit).collect();
    assert!(
        missing.is_empty() && stale.is_empty(),
        "SDK_NAMES out of date: missing {missing:?}, no longer in the WIT {stale:?}"
    );
}

#[test]
fn api_reference_is_up_to_date() {
    let page = generate();
    let path = page_path();
    if std::env::var_os("WASMKEEP_BLESS").is_some() {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, &page).unwrap();
        return;
    }
    let current = std::fs::read_to_string(&path).unwrap_or_default();
    assert!(
        current == page,
        "{} is out of date; run WASMKEEP_BLESS=1 cargo nextest run -p wasmkeep-wit --test api_reference",
        path.display()
    );
}
