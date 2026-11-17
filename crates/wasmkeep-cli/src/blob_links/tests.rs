//! The start proof against a small S3 server that checks presigned GETs
//! with its own SigV4 code (checked on AWS's published example), and the
//! same server ignoring each condition in turn: the proof must fail each
//! time, and `auto` fall back to node links.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::*;

const KEY_ID: &str = "AKIDEXAMPLE";
const SECRET: &str = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
const REGION: &str = "us-east-1";

fn hmac(key: &[u8], msg: &[u8]) -> Vec<u8> {
    let k = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, key);
    ring::hmac::sign(&k, msg).as_ref().to_vec()
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|b| format!("{b:02x}")).collect()
}

fn decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() {
            out.push(u8::from_str_radix(&s[i + 1..i + 3], 16).unwrap());
            i += 3;
        } else if b[i] == b'+' {
            out.push(b' ');
            i += 1;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8(out).unwrap()
}

fn encode(s: &str) -> String {
    s.bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || b"-_.~".contains(&b) {
                char::from(b).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect()
}

/// What the server leaves out of its checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Lax {
    Strict,
    /// Takes any signature (binds neither key nor parameters).
    NoSignature,
    /// Binds the key but not the other parameters: a signature it took
    /// once for a key, it takes again whatever the parameters.
    UnsignedParameters,
    /// Serves the stored headers, never the `response-` overrides.
    NoOverrides,
}

/// The SigV4 signature of a presigned request (query signing, unsigned
/// payload), over `path` as sent and every query parameter but the
/// signature.
fn signature(method: &str, host: &str, path: &str, query: &[(String, String)]) -> String {
    let get = |k: &str| {
        query
            .iter()
            .find(|(n, _)| n == k)
            .map(|(_, v)| v.clone())
            .unwrap_or_default()
    };
    let date = get("X-Amz-Date");
    let mut q: Vec<(String, String)> = query
        .iter()
        .filter(|(k, _)| k != "X-Amz-Signature")
        .map(|(k, v)| (encode(k), encode(v)))
        .collect();
    q.sort();
    let canonical_query = q
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");
    let canonical =
        format!("{method}\n{path}\n{canonical_query}\nhost:{host}\n\nhost\nUNSIGNED-PAYLOAD");
    let scope = format!("{}/{REGION}/s3/aws4_request", &date[..8]);
    let to_sign = format!(
        "AWS4-HMAC-SHA256\n{date}\n{scope}\n{}",
        hex(&Sha256::digest(canonical.as_bytes()))
    );
    let k = hmac(format!("AWS4{SECRET}").as_bytes(), &date.as_bytes()[..8]);
    let k = hmac(&k, REGION.as_bytes());
    let k = hmac(&k, b"s3");
    let k = hmac(&k, b"aws4_request");
    hex(&hmac(&k, to_sign.as_bytes()))
}

#[test]
fn the_servers_sigv4_is_awss() {
    // https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html
    // with its own key; the same computation as the server's below.
    let secret = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
    let q = |k: &str, v: &str| (k.to_owned(), v.to_owned());
    let query = [
        q("X-Amz-Algorithm", "AWS4-HMAC-SHA256"),
        q(
            "X-Amz-Credential",
            "AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request",
        ),
        q("X-Amz-Date", "20130524T000000Z"),
        q("X-Amz-Expires", "86400"),
        q("X-Amz-SignedHeaders", "host"),
    ];
    let mut q: Vec<(String, String)> = query.iter().map(|(k, v)| (encode(k), encode(v))).collect();
    q.sort();
    let cq = q
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");
    let canonical = format!(
        "GET\n/test.txt\n{cq}\nhost:examplebucket.s3.amazonaws.com\n\nhost\nUNSIGNED-PAYLOAD"
    );
    let to_sign = format!(
        "AWS4-HMAC-SHA256\n20130524T000000Z\n20130524/us-east-1/s3/aws4_request\n{}",
        hex(&Sha256::digest(canonical.as_bytes()))
    );
    let k = hmac(format!("AWS4{secret}").as_bytes(), b"20130524");
    let k = hmac(&hmac(&hmac(&k, b"us-east-1"), b"s3"), b"aws4_request");
    assert_eq!(
        hex(&hmac(&k, to_sign.as_bytes())),
        "aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404"
    );
}

type Objects = Arc<Mutex<HashMap<String, (Vec<u8>, String)>>>;
/// Signatures taken, and the path each was for.
type Seen = Arc<Mutex<HashMap<String, String>>>;

async fn handle(mut sock: tokio::net::TcpStream, objects: Objects, seen: Seen, lax: Lax) {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];
    let head_end = loop {
        let n = sock.read(&mut chunk).await.unwrap_or(0);
        if n == 0 {
            return;
        }
        buf.extend_from_slice(&chunk[..n]);
        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break i + 4;
        }
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).into_owned();
    let mut lines = head.split("\r\n");
    let mut first = lines.next().unwrap().split(' ');
    let (method, target) = (
        first.next().unwrap().to_owned(),
        first.next().unwrap().to_owned(),
    );
    let headers: HashMap<String, String> = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_owned()))
        .collect();
    let length: usize = headers
        .get("content-length")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let mut body = buf[head_end..].to_vec();
    while body.len() < length {
        let n = sock.read(&mut chunk).await.unwrap_or(0);
        if n == 0 {
            break;
        }
        body.extend_from_slice(&chunk[..n]);
    }
    let (path, raw_query) = target.split_once('?').unwrap_or((&target, ""));
    let query: Vec<(String, String)> = raw_query
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|p| {
            let (k, v) = p.split_once('=').unwrap_or((p, ""));
            (decode(k), decode(v))
        })
        .collect();
    let object = decode(path.trim_start_matches('/'));
    let get = |k: &str| query.iter().find(|(n, _)| n == k).map(|(_, v)| v.clone());
    let reply = |status: &str, extra: String, body: &[u8]| {
        let mut out = format!(
            "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n{extra}\r\n",
            body.len()
        )
        .into_bytes();
        out.extend_from_slice(body);
        out
    };
    let out = match method.as_str() {
        "PUT" => {
            let ty = headers.get("content-type").cloned().unwrap_or_default();
            objects.lock().unwrap().insert(object, (body, ty));
            reply("200 OK", "ETag: \"1\"\r\n".into(), b"")
        }
        "DELETE" => {
            objects.lock().unwrap().remove(&object);
            reply("204 No Content", String::new(), b"")
        }
        "GET" if get("X-Amz-Signature").is_some() => {
            let host = headers.get("host").cloned().unwrap_or_default();
            let given = get("X-Amz-Signature").unwrap();
            let valid = given == signature("GET", &host, path, &query);
            if valid {
                seen.lock().unwrap().insert(given.clone(), path.to_owned());
            }
            let date = get("X-Amz-Date").unwrap_or_default();
            let signed_at = chrono::NaiveDateTime::parse_from_str(&date, "%Y%m%dT%H%M%SZ")
                .map(|d| d.and_utc().timestamp())
                .unwrap_or(0);
            let expires: i64 = get("X-Amz-Expires")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);
            let live = chrono::Utc::now().timestamp() < signed_at + expires;
            let ok = match lax {
                Lax::NoSignature => true,
                _ if !live => false,
                Lax::UnsignedParameters => {
                    valid || seen.lock().unwrap().get(&given).is_some_and(|p| p == path)
                }
                _ => valid,
            };
            let found = objects.lock().unwrap().get(&object).cloned();
            match (ok, found) {
                (false, _) => reply("403 Forbidden", String::new(), b"SignatureDoesNotMatch"),
                (true, None) => reply("404 Not Found", String::new(), b"NoSuchKey"),
                (true, Some((bytes, stored))) => {
                    let (ty, disposition) = if lax == Lax::NoOverrides {
                        (stored, String::new())
                    } else {
                        (
                            get("response-content-type").unwrap_or(stored),
                            get("response-content-disposition").unwrap_or_default(),
                        )
                    };
                    reply(
                        "200 OK",
                        format!("Content-Type: {ty}\r\nContent-Disposition: {disposition}\r\n"),
                        &bytes,
                    )
                }
            }
        }
        _ => reply("400 Bad Request", String::new(), b""),
    };
    let _ = sock.write_all(&out).await;
}

async fn server(lax: Lax) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let objects: Objects = Arc::default();
    let seen: Seen = Arc::default();
    tokio::spawn(async move {
        loop {
            let Ok((sock, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(handle(sock, Arc::clone(&objects), Arc::clone(&seen), lax));
        }
    });
    addr
}

fn client(addr: SocketAddr) -> object_store::aws::AmazonS3 {
    object_store::aws::AmazonS3Builder::new()
        .with_bucket_name("b")
        .with_region(REGION)
        .with_access_key_id(KEY_ID)
        .with_secret_access_key(SECRET)
        .with_endpoint(format!("http://{addr}"))
        .with_allow_http(true)
        .build()
        .unwrap()
}

async fn proof(lax: Lax) -> Result<(), Failed> {
    let addr = server(lax).await;
    let s3 = Arc::new(client(addr));
    let store: Arc<dyn ObjectStore> = s3.clone();
    prove(s3.as_ref(), &store, "p/", &reqwest::Client::new()).await
}

#[tokio::test]
async fn the_proof_passes_on_a_bucket_that_checks_every_case() {
    proof(Lax::Strict).await.unwrap();
}

#[tokio::test]
async fn the_proof_fails_on_a_bucket_that_ignores_any_case() {
    for (lax, says) in [
        (Lax::NoSignature, "read another"),
        (Lax::UnsignedParameters, "was changed was taken"),
        (Lax::NoOverrides, "response-content-type was not applied"),
    ] {
        let e = proof(lax).await.unwrap_err();
        assert!(e.0.contains(says), "{lax:?}: {e}");
    }
}

#[tokio::test]
async fn auto_falls_back_to_node_links_and_presigned_refuses_to_start() {
    let with = |lax| async move {
        let addr = server(lax).await;
        let s3 = Arc::new(client(addr));
        let store: Arc<dyn ObjectStore> = s3.clone();
        (s3 as Arc<dyn Signer>, store)
    };
    let (signer, store) = with(Lax::NoSignature).await;
    let auto = presigner_with(Mode::Auto, Arc::clone(&signer), &store, "p/")
        .await
        .unwrap();
    assert!(auto.is_none(), "auto uses node links when the proof fails");
    assert!(
        presigner_with(Mode::Presigned, Arc::clone(&signer), &store, "p/")
            .await
            .is_err()
    );
    assert!(
        presigner_with(Mode::Node, signer, &store, "p/")
            .await
            .unwrap()
            .is_none()
    );
    let (signer, store) = with(Lax::Strict).await;
    assert!(
        presigner_with(Mode::Auto, signer, &store, "p/")
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn diagnose_runs_the_proof_and_an_expiry() {
    for (lax, ok) in [(Lax::Strict, true), (Lax::NoSignature, false)] {
        let addr = server(lax).await;
        let s3 = Arc::new(client(addr));
        let store: Arc<dyn ObjectStore> = s3.clone();
        let out = checks(
            s3.as_ref(),
            &store,
            "p/",
            &reqwest::Client::new(),
            (Duration::from_secs(1), Duration::from_millis(2_100)),
        )
        .await;
        let names: Vec<&str> = out.iter().map(|(n, _)| *n).collect();
        assert_eq!(names, ["presigned-download", "presigned-expiry"]);
        for (name, r) in out {
            assert_eq!(r.is_ok(), ok, "{lax:?} {name}: {r:?}");
        }
    }
}
