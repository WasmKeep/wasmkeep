// A counter actor in C#, straight on the bindings componentize-dotnet
// generates from the WasmKeep actor world (no SDK).
using System.Text;
using ActorWorld;
using ActorWorld.wit.Imports.wasmkeep.actor.v0_2_0;
using Http = ActorWorld.wit.Imports.wasi.http.v0_2_12.ITypesImports;

namespace ActorWorld
{
    public class ActorWorldExportsImpl : IActorWorldExports
    {
        public static void OnInit()
        {
            Sql.Exec("CREATE TABLE IF NOT EXISTS counter (id INTEGER PRIMARY KEY CHECK (id = 1), value INTEGER NOT NULL)");
            Sql.Exec("INSERT OR IGNORE INTO counter (id, value) VALUES (1, 0)");
        }
        public static void OnAlarm() { }
        public static void OnWebsocketOpen(ulong conn, string path) =>
            throw new WitException<string>("no WebSockets here", 0);
        public static void OnWebsocketMessage(ulong conn, IWebsocketsImports.Message msg) { }
        public static void OnWebsocketClose(ulong conn, ushort code, string reason) { }
    }

    static class Sql
    {
        // A failed statement becomes the hook's error.
        public static void Exec(string sql)
        {
            try { IStorageImports.Exec(sql, new()); }
            catch (WitException e) { throw new WitException<string>(((ITypesImports.SqlError)e.Value).message, 0); }
        }
        public static long QueryInt(string sql)
        {
            var rows = IStorageImports.Query(sql, new());
            return rows.values[0][0].AsInteger;
        }
    }
}

namespace ActorWorld.wit.Exports.wasi.http.v0_2_12
{
    public class IncomingHandlerExportsImpl : IIncomingHandlerExports
    {
        public static void Handle(Http.IncomingRequest request, Http.ResponseOutparam responseOut)
        {
            ushort status = 200;
            string body;
            var method = request.Method();
            try
            {
                if (method.Tag == Http.Method.Tags.Get)
                    body = Sql.QueryInt("SELECT value FROM counter WHERE id = 1") + "\n";
                else if (method.Tag == Http.Method.Tags.Post)
                    body = Sql.QueryInt("UPDATE counter SET value = value + 1 WHERE id = 1 RETURNING value") + "\n";
                else { status = 405; body = "use GET or POST\n"; }
            }
            catch (WitException e)
            {
                status = 500;
                body = (e.Value is ITypesImports.SqlError s ? s.message : "error") + "\n";
            }
            request.Dispose();
            var bytes = Encoding.UTF8.GetBytes(body);
            var headers = new Http.Fields();
            headers.Append("content-type", Encoding.UTF8.GetBytes("text/plain"));
            var response = new Http.OutgoingResponse(headers);
            response.SetStatusCode(status);
            var outBody = response.Body();
            Http.ResponseOutparam.Set(responseOut, Result<Http.OutgoingResponse, Http.ErrorCode>.Ok(response));
            using (var stream = outBody.Write())
                stream.BlockingWriteAndFlush(bytes);
            Http.OutgoingBody.Finish(outBody, null);
        }
    }
}
