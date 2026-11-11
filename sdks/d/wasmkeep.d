/++
WasmKeep actors in D, with LDC and `-betterC`: no garbage collector, no
D runtime, no exceptions.

An actor is a struct of static hooks, each optional, handed to the
`Actor` mixin once:

---
import wasmkeep;

struct Counter {
    static void init() {
        exec("CREATE TABLE IF NOT EXISTS hits (at INTEGER NOT NULL)");
    }
    static Response fetch(ref Request req) {
        exec("INSERT INTO hits (at) VALUES (?)", nowMs());
        return reply(200, "%lld hits\n", queryInt("SELECT count(*) FROM hits"));
    }
}

mixin Actor!Counter;
---

Hooks: `init()`, `fetch(ref Request) -> Response`, `alarm()`,
`open(ulong conn, const(char)[] path) -> bool` (false refuses),
`message(ulong conn, const(ubyte)[] data, bool binary)` and
`close(ulong conn, ushort code, const(char)[] reason)`. `fail(msg)` fails
the current `init` or `alarm` event (an alarm is retried).

Memory: everything the SDK returns (rows, text, bodies, replies) lives
until the current event ends, when the SDK frees it, and so do the
arguments hooks receive (a WebSocket path, message or close reason). Copy
what must outlive the event (with `malloc`).

The host delivers one event at a time to an actor, in arrival order, so
nothing here needs locks. The module wraps the bindings `wit-bindgen d`
generates from the actor world (`gen/wit`, `-Igen`).
+/
module wasmkeep;

import core.stdc.stdarg : va_list, va_start, va_end;
import core.stdc.stdio : vsnprintf;
import core.stdc.stdlib : malloc, realloc, free;
import core.stdc.string : memcpy, strlen;

import wit.common;
import types = wit.wasmkeep.actor.types.imports;
import storage = wit.wasmkeep.actor.storage.imports;
import alarms = wit.wasmkeep.actor.alarms.imports;
import actors = wit.wasmkeep.actor.actors.imports;
import websockets = wit.wasmkeep.actor.websockets.imports;
import envi = wit.wasmkeep.actor.env.imports;
import http = wit.wasi.http.types.imports;
import streams = wit.wasi.io.streams.imports;
import wallclock = wit.wasi.clocks.wall_clock.imports;


// The bindings take mutable lists; the host only reads them.
private WitString ws(const(char)[] s) @trusted => WitString(cast(char[]) s);
private WitList!ubyte wb(const(ubyte)[] b) @trusted => WitList!ubyte(cast(ubyte[]) b);

// ------------------------------------------------------------ the event arena

private __gshared void** owned;
private __gshared size_t ownedLen, ownedCap;

/// Allocates n bytes that live until the current event ends.
void* eventAlloc(size_t n) @trusted {
    void* p = malloc(n ? n : 1);
    if (!p) assert(0, "out of memory");
    keep(p);
    return p;
}

private void keep(void* p) @trusted {
    if (ownedLen == ownedCap) {
        ownedCap = ownedCap ? ownedCap * 2 : 64;
        owned = cast(void**) realloc(owned, ownedCap * (void*).sizeof);
        if (!owned) assert(0, "out of memory");
    }
    owned[ownedLen++] = p;
}

/// Copies s into the event arena.
const(char)[] eventCopy(const(char)[] s) @trusted {
    auto p = cast(char*) eventAlloc(s.length);
    if (s.length) memcpy(p, s.ptr, s.length);
    return p[0 .. s.length];
}

private void endEvent() @trusted {
    foreach (i; 0 .. ownedLen) free(owned[i]);
    ownedLen = 0;
}

// ------------------------------------------------------------------ errors

private __gshared char[512] errorBuf;
private __gshared size_t errorLen;
private __gshared const(char)[] errorKindName = "";

private void setError(const(char)[] kind, const(char)[] message) @trusted {
    errorKindName = kind;
    errorLen = message.length < errorBuf.length ? message.length : errorBuf.length;
    if (errorLen) memcpy(errorBuf.ptr, message.ptr, errorLen);
}

/// The message of the last failed statement or call.
const(char)[] lastError() @trusted => errorBuf[0 .. errorLen];

/// Its kind: an sql-error-kind ("syntax", "constraint", "bind",
/// "multiple-statements", "quota-exceeded", "not-allowed", "internal") or
/// a call-error case ("invalid-address", "reentrant", "too-large",
/// "timeout", "failed").
const(char)[] lastErrorKind() @trusted => errorKindName;

private __gshared const(char)[] failure;
private __gshared bool failed;

/// Fails the current `init` or `alarm` event with msg (an alarm is
/// retried). msg must outlive the hook: a literal, or `eventCopy`.
void fail(const(char)[] msg) @trusted {
    failure = msg.length ? msg : "failed";
    failed = true;
}

// ----------------------------------------------------------------- storage

/// A statement parameter: `null`, integers, `bool`, floats, `const(char)[]`
/// (text) or `const(ubyte)[]` (blob).
private types.Value toValue(T)(T v) @trusted {
    static if (is(T == typeof(null)))
        return types.Value.null_();
    else static if (is(T == bool))
        return types.Value.integer(v ? 1 : 0);
    else static if (__traits(isIntegral, T))
        return types.Value.integer(cast(long) v);
    else static if (__traits(isFloating, T))
        return types.Value.real_(cast(double) v);
    else static if (is(T : const(char)[]))
        return types.Value.text(ws(v));
    else static if (is(T : const(ubyte)[]))
        return types.Value.blob(wb(cast(const(ubyte)[]) v));
    else
        static assert(0, "unsupported parameter type " ~ T.stringof);
}

private auto withParams(alias F, Args...)(const(char)[] sql, Args args) @trusted {
    types.Value[Args.length ? Args.length : 1] values = void;
    static foreach (i, A; Args) values[i] = toValue(args[i]);
    return F(ws(sql), WitList!(types.Value)(values[0 .. Args.length]));
}

private const(char)[] kindName(types.SqlErrorKind k) {
    final switch (k) {
        case types.SqlErrorKind.syntax: return "syntax";
        case types.SqlErrorKind.constraint: return "constraint";
        case types.SqlErrorKind.bind: return "bind";
        case types.SqlErrorKind.multipleStatements: return "multiple-statements";
        case types.SqlErrorKind.quotaExceeded: return "quota-exceeded";
        case types.SqlErrorKind.notAllowed: return "not-allowed";
        case types.SqlErrorKind.internal: return "internal";
    }
}

private long changed(R)(ref R r) @trusted {
    if (r.isErr) {
        auto e = r.unwrapErr;
        setError(kindName(e.kind), e.message[]);
        e.witFree;
        return -1;
    }
    return cast(long) r.unwrap;
}

/// Runs one statement that returns no rows: the rows changed, or -1 (then
/// `lastError`, `lastErrorKind`). Committed to the local database.
long exec(Args...)(const(char)[] sql, Args args) @trusted {
    auto r = withParams!(storage.exec)(sql, args);
    return changed(r);
}

/// `exec`, returning only once the write is durable: replicated to the
/// bucket when the node has one, fsynced locally when it does not. Inside
/// a transaction it takes only COMMIT (or END).
long execDurable(Args...)(const(char)[] sql, Args args) @trusted {
    auto r = withParams!(storage.execDurable)(sql, args);
    return changed(r);
}

/// A query's result, valid until the event ends. `ok` is false when the
/// statement failed (then `lastError`).
struct Rows {
    bool ok;
    private types.Rows raw;

    /// The number of rows.
    size_t count() const => raw.values.length;
    /// The number of columns.
    size_t width() const => raw.columns.length;
    /// The name of column col.
    const(char)[] columnName(size_t col) const @trusted =>
        col < raw.columns.length ? raw.columns[col][] : null;
    /// The index of the column name, or -1.
    ptrdiff_t column(const(char)[] name) const @trusted {
        foreach (i, c; raw.columns[]) if (c[] == name) return i;
        return -1;
    }
    /// Whether the value at row, col is NULL (or past the end).
    bool isNull(size_t row, size_t col) const @trusted {
        auto v = at(row, col);
        return v is null || v.tag == types.Value.Tag.null_;
    }
    /// The value at row, col as an integer: a REAL is truncated, anything
    /// else 0.
    long getInt(size_t row, size_t col) const @trusted {
        auto v = at(row, col);
        if (v is null) return 0;
        if (v.tag == types.Value.Tag.integer) return v.getInteger;
        if (v.tag == types.Value.Tag.real_) return cast(long) v.getReal;
        return 0;
    }
    /// The value at row, col as a double: an INTEGER converted, anything
    /// else 0.
    double getReal(size_t row, size_t col) const @trusted {
        auto v = at(row, col);
        if (v is null) return 0;
        if (v.tag == types.Value.Tag.real_) return v.getReal;
        if (v.tag == types.Value.Tag.integer) return cast(double) v.getInteger;
        return 0;
    }
    /// The value at row, col as text (a BLOB's bytes too); null otherwise.
    const(char)[] getText(size_t row, size_t col) const @trusted {
        auto v = at(row, col);
        if (v is null) return null;
        if (v.tag == types.Value.Tag.text) return v.getText[];
        if (v.tag == types.Value.Tag.blob) return cast(const(char)[]) v.getBlob[];
        return null;
    }
    /// The value at row, col as bytes (TEXT's too); null otherwise.
    const(ubyte)[] getBlob(size_t row, size_t col) const @trusted =>
        cast(const(ubyte)[]) getText(row, col);

    private const(types.Value)* at(size_t row, size_t col) const @trusted {
        if (row >= raw.values.length) return null;
        auto r = raw.values[row][];
        return col < r.length ? &r[col] : null;
    }
}

private __gshared types.Rows[] rowsToFree;

/// Runs one statement and returns its rows.
Rows query(Args...)(const(char)[] sql, Args args) @trusted {
    auto r = withParams!(storage.query)(sql, args);
    Rows rows;
    if (r.isErr) {
        auto e = r.unwrapErr;
        setError(kindName(e.kind), e.message[]);
        e.witFree;
        return rows;
    }
    rows.ok = true;
    rows.raw = r.unwrap;
    // Freed with the event: keep the lists' storage, then the rows.
    auto keepRows = cast(types.Rows*) malloc(types.Rows.sizeof);
    if (!keepRows) assert(0, "out of memory");
    *keepRows = rows.raw;
    pendingRows(keepRows);
    return rows;
}

private __gshared types.Rows** rowsList;
private __gshared size_t rowsLen, rowsCap;

private void pendingRows(types.Rows* r) @trusted {
    if (rowsLen == rowsCap) {
        rowsCap = rowsCap ? rowsCap * 2 : 16;
        rowsList = cast(types.Rows**) realloc(rowsList, rowsCap * (types.Rows*).sizeof);
        if (!rowsList) assert(0, "out of memory");
    }
    rowsList[rowsLen++] = r;
}

private void freeRows() @trusted {
    foreach (i; 0 .. rowsLen) {
        rowsList[i].witFree;
        free(rowsList[i]);
    }
    rowsLen = 0;
}

/// The first column of the first row as an integer; 0 when the query
/// failed or returned nothing (check `lastError` if that matters).
long queryInt(Args...)(const(char)[] sql, Args args) @trusted {
    auto rows = query(sql, args);
    return rows.ok && rows.count ? rows.getInt(0, 0) : 0;
}

/// Deletes this actor once the current event ends: its database is
/// removed and its alarm cleared.
void deleteSelf() @trusted { storage.deleteSelf(); }

// ------------------------------------------------------------------ alarms

/// Schedules `alarm` at ms (milliseconds since the Unix epoch), replacing
/// any pending alarm. Alarms fire at least once.
void setAlarm(long ms) @trusted { alarms.setAlarm(ms < 0 ? 0 : cast(ulong) ms); }

/// Whether an alarm is pending, and when (into ms).
bool getAlarm(out long ms) @trusted {
    auto at = alarms.getAlarm();
    if (at.isNone) return false;
    ms = cast(long) at.unwrap;
    return true;
}

/// Cancels the pending alarm, if any.
void clearAlarm() @trusted { alarms.clearAlarm(); }

/// Milliseconds since the Unix epoch, from the host's wall clock.
long nowMs() @trusted {
    auto t = wallclock.now();
    return cast(long) t.seconds * 1000 + t.nanoseconds / 1_000_000;
}

// --------------------------------------------------------------------- env

/// The class's configuration value or secret name, or null when it is
/// not set (valid until the event ends).
const(char)[] env(const(char)[] name) @trusted {
    auto v = envi.get(ws(name));
    if (v.isNone) return null;
    auto s = v.unwrap;
    auto copy = eventCopy(s[]);
    s.witFree;
    return copy.ptr ? copy : "";
}

// ------------------------------------------------------------------ actors

/// An actor's address; Class ~ "/" ~ name is what `call` takes.
struct Address {
    const(char)[] tenant, class_, name;
}

/// This actor's own address (valid until the event ends).
Address self() @trusted {
    auto a = actors.selfAddress();
    Address out_ = Address(eventCopy(a.tenant[]), eventCopy(a.class_[]), eventCopy(a.name[]));
    a.witFree;
    return out_;
}

/// A header, name and value.
struct Header {
    const(char)[] name, value;
}

/// Another actor's answer. `ok` is false when the call failed (then
/// `lastError`, `lastErrorKind`).
struct CallResponse {
    bool ok;
    ushort status;
    const(ubyte)[] body;
    const(Header)[] headers;
}

/// Calls the actor at address (`class/name`, same tenant) and waits for
/// its answer; other events to this actor wait meanwhile.
CallResponse call(const(char)[] address, const(char)[] method, const(char)[] path,
        const(ubyte)[] body = null, const(Header)[] headers = null) @trusted {
    auto hs = cast(Tuple!(WitString, WitString)*) eventAlloc(headers.length * Tuple!(WitString, WitString).sizeof);
    foreach (i, h; headers) hs[i] = tuple(ws(h.name), ws(h.value));
    auto req = actors.Request(ws(method), ws(path),
        WitList!(Tuple!(WitString, WitString))(hs[0 .. headers.length]), wb(cast(const(ubyte)[]) body));
    auto r = actors.call(ws(address), req);
    CallResponse out_;
    if (r.isErr) {
        auto e = r.unwrapErr;
        final switch (e.tag) {
            case actors.CallError.Tag.invalidAddress: setError("invalid-address", e.getInvalidAddress[]); break;
            case actors.CallError.Tag.reentrant: setError("reentrant", "the call would re-enter an actor in this chain"); break;
            case actors.CallError.Tag.tooLarge: setError("too-large", "body too large"); break;
            case actors.CallError.Tag.timeout: setError("timeout", "the callee did not answer in time"); break;
            case actors.CallError.Tag.failed: setError("failed", e.getFailed[]); break;
        }
        e.witFree;
        return out_;
    }
    auto resp = r.unwrap;
    out_.ok = true;
    out_.status = resp.status;
    out_.body = cast(const(ubyte)[]) eventCopy(cast(const(char)[]) resp.body_[]);
    auto outHeaders = cast(Header*) eventAlloc(resp.headers.length * Header.sizeof);
    foreach (i, h; resp.headers[]) outHeaders[i] = Header(eventCopy(h[0][]), eventCopy(h[1][]));
    out_.headers = outHeaders[0 .. resp.headers.length];
    resp.witFree;
    return out_;
}

// -------------------------------------------------------------- websockets

/// Queues a text message on conn; false if the connection is gone or the
/// client is not reading fast enough (the message was dropped).
bool send(ulong conn, const(char)[] text) @trusted =>
    websockets.send(conn, websockets.Message.text(ws(text))).isOk;

/// Queues a binary message on conn.
bool sendBinary(ulong conn, const(ubyte)[] data) @trusted =>
    websockets.send(conn, websockets.Message.binary(wb(cast(const(ubyte)[]) data))).isOk;

/// Sends text to every open connection of this actor except `except` (0:
/// none).
void broadcast(const(char)[] text, ulong except = 0) @trusted {
    foreach (conn; connections()) if (conn != except) send(conn, text);
}

/// Closes conn with a close code (1000 is normal) and a reason.
void close(ulong conn, ushort code = 1000, const(char)[] reason = "") @trusted {
    websockets.close(conn, code, ws(reason));
}

/// This actor's open connections, oldest first (valid until the event
/// ends).
const(ulong)[] connections() @trusted {
    auto list = websockets.connections();
    auto copy = cast(ulong*) eventAlloc(list.length * ulong.sizeof);
    if (list.length) memcpy(copy, list.ptr, list.length * ulong.sizeof);
    auto n = list.length;
    list.witFree;
    return copy[0 .. n];
}

// -------------------------------------------------------------------- HTTP

/// An incoming request (or a call from another actor). The path is
/// relative to the actor and includes the query.
struct Request {
    const(char)[] method;
    const(char)[] path;
    const(Header)[] headers;
    private http.IncomingRequest handle;
    private bool bodyRead;
    private const(ubyte)[] bodyBytes;

    /// The first value of header name (any case), or null.
    const(char)[] header(const(char)[] name) const @trusted {
        foreach (h; headers) if (equalFold(h.name, name)) return h.value;
        return null;
    }

    /// The whole body, read on first use.
    const(ubyte)[] body() @trusted {
        if (!bodyRead) {
            bodyRead = true;
            bodyBytes = readBody(handle);
        }
        return bodyBytes;
    }

    /// The body as text.
    const(char)[] text() @trusted => cast(const(char)[]) body();
}

/// What `fetch` answers. Without a content type it is
/// `text/plain; charset=utf-8`.
struct Response {
    ushort status = 200;
    const(char)[] contentType;
    const(ubyte)[] body;
    const(Header)[] headers;
}

/// A response whose body is printf-formatted (`%lld` for a long, `%.*s`
/// with `cast(int) s.length, s.ptr` for a slice).
pragma(printf)
extern (C) Response reply(ushort status, const(char)* fmt, ...) @trusted {
    va_list args;
    va_start(args, fmt);
    va_list copy;
    import core.stdc.stdarg : va_copy;
    va_copy(copy, args);
    int n = vsnprintf(null, 0, fmt, copy);
    va_end(copy);
    Response r;
    r.status = status;
    if (n > 0) {
        auto buf = cast(char*) eventAlloc(n + 1);
        vsnprintf(buf, n + 1, fmt, args);
        r.body = cast(const(ubyte)[]) buf[0 .. n];
    }
    va_end(args);
    return r;
}

/// A response with body as it is (not copied: it must outlive the event's
/// hook, as a literal or arena memory does).
Response replyText(ushort status, const(char)[] body, const(char)[] contentType = null) {
    return Response(status, contentType, cast(const(ubyte)[]) body);
}

/// The response with one more header (the list is copied to the arena).
Response withHeader(Response r, const(char)[] name, const(char)[] value) @trusted {
    auto hs = cast(Header*) eventAlloc((r.headers.length + 1) * Header.sizeof);
    if (r.headers.length) memcpy(hs, r.headers.ptr, r.headers.length * Header.sizeof);
    hs[r.headers.length] = Header(name, value);
    r.headers = hs[0 .. r.headers.length + 1];
    return r;
}

private bool equalFold(const(char)[] a, const(char)[] b) {
    if (a.length != b.length) return false;
    foreach (i; 0 .. a.length) {
        char x = a[i], y = b[i];
        if (x >= 'A' && x <= 'Z') x += 32;
        if (y >= 'A' && y <= 'Z') y += 32;
        if (x != y) return false;
    }
    return true;
}

private immutable string[9] methodNames = ["GET", "HEAD", "POST", "PUT", "DELETE", "CONNECT", "OPTIONS", "TRACE", "PATCH"];

private const(ubyte)[] readBody(http.IncomingRequest req) @trusted {
    auto consumed = req.borrow.consume();
    if (consumed.isErr) return null;
    auto incoming = consumed.unwrap;
    auto opened = incoming.borrow.stream();
    if (opened.isErr) {
        incoming.witDrop;
        return null;
    }
    auto stream = opened.unwrap;
    ubyte* buf = null;
    size_t len = 0;
    for (;;) {
        auto chunk = stream.borrow.blockingRead(65536);
        if (chunk.isErr) {
            auto e = chunk.unwrapErr;
            e.witDrop;
            break; // closed: the body has ended
        }
        auto bytes = chunk.unwrap;
        if (bytes.length) {
            buf = cast(ubyte*) realloc(buf, len + bytes.length);
            if (!buf) assert(0, "out of memory");
            memcpy(buf + len, bytes.ptr, bytes.length);
            len += bytes.length;
        }
        bytes.witFree;
    }
    stream.witDrop;
    http.IncomingBody.finish(incoming).witDrop;
    if (buf) keep(buf);
    return buf ? buf[0 .. len] : null;
}

private Request incoming(http.IncomingRequest handle) @trusted {
    Request r;
    r.handle = handle;
    auto m = handle.borrow.method();
    if (m.tag == http.Method.Tag.other) {
        r.method = eventCopy(m.getOther[]);
    } else {
        r.method = methodNames[m.tag];
    }
    m.witFree;
    auto p = handle.borrow.pathWithQuery();
    if (p.isSome) {
        auto s = p.unwrap;
        r.path = eventCopy(s[]);
        s.witFree;
    } else {
        r.path = "/";
    }
    auto fields = handle.borrow.headers();
    auto entries = fields.borrow.entries();
    auto hs = cast(Header*) eventAlloc(entries.length * Header.sizeof);
    foreach (i, ref e; entries[]) {
        hs[i] = Header(eventCopy(e[0][]), eventCopy(cast(const(char)[]) e[1][]));
    }
    r.headers = hs[0 .. entries.length];
    entries.witFree;
    fields.witDrop;
    return r;
}

private void respond(http.ResponseOutparam out_, ref Response r) @trusted {
    auto fields = http.Fields.makeNew();
    auto type = r.contentType.length ? r.contentType : "text/plain; charset=utf-8";
    appendField(fields, "content-type", type);
    foreach (h; r.headers) appendField(fields, h.name, h.value);
    auto resp = http.OutgoingResponse.makeNew(fields);
    cast(void) resp.borrow.setStatusCode(r.status);
    auto bodyResult = resp.borrow.body_();
    http.ResponseOutparam.set(out_, Result!(http.OutgoingResponse, http.ErrorCode).makeOk(resp));
    if (bodyResult.isErr) return;
    auto outBody = bodyResult.unwrap;
    auto written = outBody.borrow.write();
    if (written.isOk) {
        auto stream = written.unwrap;
        // blocking-write-and-flush takes at most 4096 bytes at a time.
        for (size_t at = 0; at < r.body.length; at += 4096) {
            auto end = at + 4096 < r.body.length ? at + 4096 : r.body.length;
            auto w = stream.borrow.blockingWriteAndFlush(wb(cast(const(ubyte)[]) r.body[at .. end]));
            if (w.isErr) {
                auto e = w.unwrapErr;
                e.witDrop;
                break;
            }
        }
        stream.witDrop;
    }
    auto finished = http.OutgoingBody.finish(outBody, Option!(http.Trailers).makeNone());
    if (finished.isErr) {
        auto e = finished.unwrapErr;
        e.witFree;
    }
}

private void appendField(http.Fields f, const(char)[] name, const(char)[] value) @trusted {
    auto r = f.borrow.append(ws(name), wb(cast(const(ubyte)[]) value));
    if (r.isErr) {
        auto e = r.unwrapErr;
        e.witFree;
    }
}

// ------------------------------------------------------------------- hooks

/// Exports the hooks of H (a struct of static functions, each optional)
/// as the component's: mix it into the app's module once.
mixin template Actor(H) {
    static import wasmkeep;
    static import wit.common;
    static import wit.wasmkeep.actor.actor;
    static import wit.wasmkeep.actor.websockets.imports;
    static import wit.wasi.http.types.imports;

    private struct __WasmKeepExports {
    static:
        @(wit.common.witExport("on-init"))
        @(wit.common.witInterface("$root"))
        wit.common.Result!(void, wit.common.WitString) onInit() {
            static if (__traits(hasMember, H, "init")) H.init();
            return wasmkeep.__finishHook();
        }

        @(wit.common.witExport("on-alarm"))
        @(wit.common.witInterface("$root"))
        wit.common.Result!(void, wit.common.WitString) onAlarm() {
            static if (__traits(hasMember, H, "alarm")) H.alarm();
            return wasmkeep.__finishHook();
        }

        @(wit.common.witExport("on-websocket-open"))
        @(wit.common.witInterface("$root"))
        wit.common.Result!(void, wit.common.WitString) onWebsocketOpen(ulong conn, in wit.common.WitString path) {
            static if (__traits(hasMember, H, "open")) {
                if (!H.open(conn, path[])) wasmkeep.fail("refused");
            } else static if (!__traits(hasMember, H, "message")) {
                wasmkeep.fail("this actor takes no WebSockets");
            }
            auto result = wasmkeep.__finishHook(); // copies a failure first: it may point into path
            wasmkeep.__freeArg(path.ptr);
            return result;
        }

        @(wit.common.witExport("on-websocket-message"))
        @(wit.common.witInterface("$root"))
        void onWebsocketMessage(ulong conn, in wit.wasmkeep.actor.websockets.imports.Message msg) {
            static if (__traits(hasMember, H, "message")) {
                if (msg.tag == wit.wasmkeep.actor.websockets.imports.Message.Tag.text) H.message(conn, cast(const(ubyte)[]) msg.getText[], false);
                else H.message(conn, msg.getBinary[], true);
            }
            if (msg.tag == wit.wasmkeep.actor.websockets.imports.Message.Tag.text) wasmkeep.__freeArg(msg.getText.ptr);
            else wasmkeep.__freeArg(msg.getBinary.ptr);
            wasmkeep.__endHook();
        }

        @(wit.common.witExport("on-websocket-close"))
        @(wit.common.witInterface("$root"))
        void onWebsocketClose(ulong conn, ushort code, in wit.common.WitString reason) {
            static if (__traits(hasMember, H, "close")) H.close(conn, code, reason[]);
            wasmkeep.__freeArg(reason.ptr);
            wasmkeep.__endHook();
        }

        @(wit.common.witExport("handle"))
        @(wit.common.witInterface("wasi:http/incoming-handler@0.2.12"))
        void handle(wit.wasi.http.types.imports.IncomingRequest request,
                wit.wasi.http.types.imports.ResponseOutparam responseOut) {
            static if (__traits(hasMember, H, "fetch")) {
                wasmkeep.__serve!(H.fetch)(request, responseOut);
            } else {
                wasmkeep.__serve!(wasmkeep.__notFound)(request, responseOut);
            }
        }
    }

    alias __wasmkeepExports = wit.wasmkeep.actor.actor.Exports!(wit.common.witExportsIn!__WasmKeepExports);
}

/// (For the Actor mixin.)
Result!(void, WitString) __finishHook() @trusted {
    if (failed) {
        failed = false;
        // A copy the generated post-return function frees once the host
        // has read it.
        auto msg = cast(char*) malloc(failure.length);
        if (!msg) assert(0, "out of memory");
        memcpy(msg, failure.ptr, failure.length);
        auto err = WitString(msg[0 .. failure.length]);
        __endHook();
        return Result!(void, WitString).makeErr(err);
    }
    __endHook();
    return Result!(void, WitString).makeOk();
}

/// (For the Actor mixin.) Frees an argument the host passed to an
/// export: the generated wrappers leave that to the implementation.
void __freeArg(const(void)* p) @trusted {
    if (p) free(cast(void*) p);
}

/// (For the Actor mixin.)
void __endHook() @trusted {
    freeRows();
    endEvent();
}

/// (For the Actor mixin.)
Response __notFound(ref Request) => replyText(404, "no fetch handler\n");

/// (For the Actor mixin.)
void __serve(alias fetch)(http.IncomingRequest request, http.ResponseOutparam out_) @trusted {
    auto req = incoming(request);
    Response r = fetch(req);
    respond(out_, r);
    request.witDrop;
    __endHook();
}
