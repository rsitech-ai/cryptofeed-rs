//! Actual local TLS handshakes; synthetic test-only cert/key, never public I/O.
use super::*;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use std::{sync::Arc, time::Duration};
const CERT: &[u8] = include_bytes!("../tests/fixtures/localhost-cert.der");
const TEST_KEY: &[u8] = include_bytes!("../tests/fixtures/localhost-test-only-key.der");
async fn server() -> (u16, tokio::task::JoinHandle<bool>) {
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(TEST_KEY.to_vec()));
    let config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(vec![CertificateDer::from(CERT.to_vec())], key)
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let task = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_secs(5), async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let result = tokio_rustls::TlsAcceptor::from(Arc::new(config))
                .accept(tcp)
                .await;
            match result {
                Ok(tls) => {
                    let mut socket = tokio_tungstenite::accept_async(tls).await.unwrap();
                    let _ = socket.close(None).await;
                    true
                }
                Err(_) => false,
            }
        })
        .await
        .expect("TLS fixture server stalled")
    });
    (port, task)
}
fn test_connector() -> Connector {
    let mut roots = rustls::RootCertStore::empty();
    roots.add(CertificateDer::from(CERT.to_vec())).unwrap();
    Connector::Rustls(Arc::new(tls_config(roots).unwrap()))
}
#[tokio::test]
async fn production_connector_rejects_self_signed_loopback_certificate() {
    let (port, server) = server().await;
    let mut ws = TungsteniteWebSocket::new_causal();
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        ws.connect(&WebSocketSpec {
            url: format!("wss://localhost:{port}"),
            ..WebSocketSpec::default()
        }),
    )
    .await
    .unwrap();
    let error = result.unwrap_err();
    assert!(
        matches!(&error,TransportError::Io(detail)|TransportError::Tls(detail) if detail.contains("UnknownIssuer")),
        "wrong rejection cause: {error:?}"
    );
    assert!(!server.await.unwrap());
    assert!(ws.stream.is_none());
}
#[tokio::test]
async fn normal_verifier_accepts_trusted_name_and_rejects_wrong_name() {
    for correct in [true, false] {
        let (port, server) = server().await;
        let host = if correct { "localhost" } else { "127.0.0.1" };
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            connect_async_tls_with_config(
                format!("wss://{host}:{port}"),
                None,
                false,
                Some(test_connector()),
            ),
        )
        .await
        .unwrap();
        assert_eq!(result.is_ok(), correct);
        if !correct {
            let error = result.unwrap_err();
            assert!(
                format!("{error:?}").contains("NotValidForName"),
                "wrong rejection cause: {error:?}"
            );
        }
        assert_eq!(server.await.unwrap(), correct);
    }
}
#[test]
fn bundled_roots_exclude_distrusted_emsign_ecc_g3() {
    assert!(!webpki_roots::TLS_SERVER_ROOTS.is_empty());
    let distrusted = b"emSign ECC Root CA - G3";
    assert!(!webpki_roots::TLS_SERVER_ROOTS.iter().any(|root| {
        root.subject
            .as_ref()
            .windows(distrusted.len())
            .any(|part| part == distrusted)
    }));
}
