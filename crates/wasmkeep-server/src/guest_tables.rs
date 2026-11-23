//! Caps on the host memory a guest can make the host hold for it outside
//! its linear memory: the store's resource table (every handle a guest
//! holds, `wasi:http` `fields` among them) and the size of one `fields`
//! the guest builds. The actors' stores and the front's are built with
//! these.

use wasmtime::component::ResourceTable;
use wasmtime_wasi_http::WasiHttpCtx;

/// Host resources one store may hold at once (wasmtime's default is
/// 1,000,000). An event holds its request, headers, body and streams, its
/// response and body, the pollables it waits on, and about ten more per
/// outbound request (at most 16 in flight per actor) or open blob stream.
/// The examples and the language tests peak at about 150 in one event
/// (jobs and outbound fetch fan-outs), and a resident actor stays flat
/// across thousands of events. Past the cap, the call that would add one
/// more traps: the event fails, and an actor's instance is discarded (the
/// next event starts a fresh one).
pub const MAX_GUEST_RESOURCES: usize = 4096;

/// Accounted bytes of one `fields` a guest builds, or copies to change
/// (`wasi:http`'s default is 128 KiB). Each name and value counts its
/// bytes plus a few dozen for its in-memory structure, so 64 KiB holds
/// several times the 8 to 32 KiB of headers common proxies and browsers
/// accept. Past it, the call traps, as past the table's cap (`wasi:http`
/// 0.2 has no header error for it). Headers that arrive from the network
/// (a request, a fetched response) are bounded by the HTTP parser instead;
/// a guest that copies those to change them is held to this size.
pub const MAX_GUEST_FIELDS_BYTES: usize = 64 * 1024;

/// A resource table capped at [`MAX_GUEST_RESOURCES`].
pub(crate) fn resource_table() -> ResourceTable {
    let mut table = ResourceTable::new();
    table.set_max_capacity(MAX_GUEST_RESOURCES);
    table
}

/// A `wasi:http` context whose guest-built `fields` stop at
/// [`MAX_GUEST_FIELDS_BYTES`].
pub(crate) fn http_ctx() -> WasiHttpCtx {
    let mut http = WasiHttpCtx::new();
    http.set_field_size_limit(MAX_GUEST_FIELDS_BYTES);
    http
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_table_refuses_past_its_cap() {
        let mut table = resource_table();
        assert_eq!(table.max_capacity(), MAX_GUEST_RESOURCES);
        let mut held: Vec<_> = (0..MAX_GUEST_RESOURCES)
            .map(|i| table.push(i).expect("under the cap"))
            .collect();
        assert!(table.push(0usize).is_err(), "one past the cap");
        table.delete(held.pop().unwrap()).unwrap();
        assert!(table.push(0usize).is_ok(), "room again once one is dropped");
    }
}
