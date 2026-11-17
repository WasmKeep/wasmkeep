use wasmkeep_sdk::directory::{self, Change, Client, Error, Keep};
use wasmkeep_sdk::{Actor, actors, calls, http};
struct TestDirectory;
impl Actor for TestDirectory {
    fn on_init() -> Result<(), String> {
        directory::handler::init().map_err(|e| e.to_string())
    }
}
fn check() -> Result<(), Error> {
    let c = Client::new("directory/map");
    assert!(matches!(c.lookup(0), Err(Error::NotFound)));
    let original = c.create("edge/a", b"left")?;
    assert_eq!(original.version, 1);
    assert_eq!(c.create("edge/ignored", b"")?, original);
    let split = Change::Split {
        lo: 0,
        at: 1 << 63,
        target: "edge/b".into(),
        meta: b"right".to_vec(),
        expect: 1,
        op: "split-1".into(),
    };
    // Two requests to the same actor in a real call-many. The lookup can
    // run before or after the split, but must see a complete range.
    let mut split_body = Vec::new();
    split_body.extend_from_slice(&0u64.to_be_bytes());
    split_body.extend_from_slice(&(1u64 << 63).to_be_bytes());
    for b in [b"edge/b".as_slice(), b"right"] {
        split_body.extend_from_slice(&(b.len() as u32).to_be_bytes());
        split_body.extend_from_slice(b);
    }
    split_body.extend_from_slice(&1u64.to_be_bytes());
    split_body.extend_from_slice(&7u32.to_be_bytes());
    split_body.extend_from_slice(b"split-1");
    let req = |op: &str, body: Vec<u8>| actors::Request {
        method: "POST".into(),
        path: format!("/wasmkeep-directory/v1/{op}"),
        headers: vec![],
        body,
    };
    let replies = calls::call_many(&[
        ("directory/map".into(), req("split", split_body)),
        (
            "directory/map".into(),
            req("lookup", u64::MAX.to_be_bytes().to_vec()),
        ),
    ]);
    for r in &replies {
        assert_eq!(r.as_ref().unwrap().status, 200);
    }
    let lookup = &replies[1].as_ref().unwrap().body;
    let version = u64::from_be_bytes(lookup[0..8].try_into().unwrap());
    let lo = u64::from_be_bytes(lookup[8..16].try_into().unwrap());
    assert!(version == 1 && lo == 0 || version == 2 && lo == 1 << 63);
    assert_eq!(&lookup[16..32], &[0; 16]); // unbounded upper range, never a half-map
    let b = c.change(&split)?;
    assert_eq!(b.version, 2);
    assert_eq!(b.entries.len(), 2);
    assert_eq!(c.lookup(u64::MAX)?.0.target, "edge/b");
    assert!(matches!(
        c.change(&Change::Merge {
            lo: 1 << 63,
            keep: Keep::Right,
            expect: 1,
            op: "bad".into()
        }),
        Err(Error::Conflict(2))
    ));
    let merge = Change::Merge {
        lo: 1 << 63,
        keep: Keep::Right,
        expect: 2,
        op: "merge-1".into(),
    };
    let merged = c.change(&merge)?;
    assert_eq!(merged.entries.len(), 1);
    assert_eq!(merged.entries[0].target, "edge/b");
    assert_eq!(c.change(&merge)?, merged);
    assert_eq!(c.lookup(0)?.0.meta, b"right");
    assert!(matches!(c.delete(2), Err(Error::Conflict(3))));
    c.delete(3)?;
    assert!(matches!(c.entries(), Err(Error::NotFound)));
    Ok(())
}
impl http::Guest for TestDirectory {
    fn handle(req: http::IncomingRequest, out: http::ResponseOutparam) {
        if req.path_with_query().as_deref() == Some("/test") {
            match check() {
                Ok(()) => http::respond(out, 200, "text/plain", b"ok"),
                Err(e) => http::respond(out, 500, "text/plain", e.to_string().as_bytes()),
            }
        } else {
            directory::handler::handle(req, out);
        }
    }
}
wasmkeep_sdk::export!(TestDirectory);
