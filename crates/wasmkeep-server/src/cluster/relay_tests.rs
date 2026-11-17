#![allow(clippy::unwrap_used)]
use super::*;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

trait Transport: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Transport for T {}
type Stream = Box<dyn Transport>;

async fn pair(tls: bool) -> (Stream, Stream) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (client, server) = tokio::join!(
        tokio::net::TcpStream::connect(listener.local_addr().unwrap()),
        listener.accept()
    );
    let (client, server) = (client.unwrap(), server.unwrap().0);
    if !tls {
        return (Box::new(client), Box::new(server));
    }
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let client_config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    let server_config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(
        vec![cert.cert.der().clone()],
        rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der()).into(),
    )
    .unwrap();
    let connector = tokio_rustls::TlsConnector::from(Arc::new(client_config));
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_config));
    let (client, server) = tokio::join!(
        connector.connect("localhost".try_into().unwrap(), client),
        acceptor.accept(server)
    );
    (Box::new(client.unwrap()), Box::new(server.unwrap()))
}

async fn relay(
    tls: bool,
) -> (
    Stream,
    Stream,
    Arc<crate::SocketBudget>,
    tokio::task::JoinHandle<()>,
) {
    let (client, mut a) = pair(tls).await;
    let (owner, mut b) = pair(tls).await;
    let budget = crate::SocketBudget::new(1, 1);
    let permit = budget.reserve().unwrap();
    let task = tokio::spawn(async move {
        let _permit = permit;
        relay_until_eof(&mut a, &mut b, Duration::from_secs(1)).await;
    });
    (client, owner, budget, task)
}

async fn client_half_close(tls: bool) {
    let (mut client, mut owner, budget, task) = relay(tls).await;
    let close = [0x88, 0x02, 0x03, 0xe8];
    let mut tail = vec![0x5a; 4 * 1024 * 1024];
    tail.extend_from_slice(&close);
    let expected = tail.clone();
    client.write_all(&close).await.unwrap();
    client.shutdown().await.unwrap();
    let sender = tokio::spawn(async move {
        let mut request = Vec::new();
        owner.read_to_end(&mut request).await.unwrap();
        assert_eq!(request, close);
        // A tail queued by the owner can take longer than the old 250 ms grace.
        tokio::time::sleep(Duration::from_millis(400)).await;
        owner.write_all(&tail).await.unwrap();
        owner.shutdown().await.unwrap();
    });
    let mut received = Vec::new();
    tokio::time::timeout(Duration::from_secs(3), client.read_to_end(&mut received))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(received.len(), expected.len(), "owner tail was truncated");
    assert_eq!(received, expected);
    sender.await.unwrap();
    task.await.unwrap();
    assert_eq!(budget.in_use(), 0);
}

async fn owner_close(tls: bool, idle: bool) {
    let (client, mut owner, budget, task) = relay(tls).await;
    let (mut read, mut write) = tokio::io::split(client);
    let close = [0x88, 0x02, 0x03, 0xe8];
    let started = tokio::time::Instant::now();
    let sender = tokio::spawn(async move {
        if !idle {
            for _ in 0..100 {
                if write.write_all(&[0x5a; 8192]).await.is_err() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            let _ = write.shutdown().await;
        } else {
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    });
    owner.write_all(&close).await.unwrap();
    owner.shutdown().await.unwrap();
    let drain = tokio::spawn(async move {
        let mut bytes = Vec::new();
        let _ = owner.read_to_end(&mut bytes).await;
    });
    let mut received = Vec::new();
    tokio::time::timeout(Duration::from_millis(200), read.read_to_end(&mut received))
        .await
        .expect("owner FIN must propagate promptly")
        .unwrap();
    assert_eq!(received, close);
    if idle {
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            budget.in_use(),
            1,
            "wind-down must use close_wait, not 250 ms"
        );
    }
    tokio::time::timeout(Duration::from_millis(1500), task)
        .await
        .unwrap()
        .unwrap();
    assert!(started.elapsed() < Duration::from_millis(1800));
    assert_eq!(budget.in_use(), 0);
    sender.abort();
    drain.abort();
}

#[tokio::test]
async fn relay_plain_client_half_close_tail() {
    client_half_close(false).await;
}
#[tokio::test]
async fn relay_tls_client_half_close_tail() {
    client_half_close(true).await;
}
#[tokio::test]
async fn relay_plain_owner_close_while_sending() {
    owner_close(false, false).await;
}
#[tokio::test]
async fn relay_tls_owner_close_while_sending() {
    owner_close(true, false).await;
}
#[tokio::test]
async fn relay_plain_idle_permit_bound() {
    owner_close(false, true).await;
}
#[tokio::test]
async fn relay_tls_idle_permit_bound() {
    owner_close(true, true).await;
}
