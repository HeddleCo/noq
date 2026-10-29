// SPDX-License-Identifier: Apache-2.0
#![cfg(all(feature = "runtime-tokio", feature = "rustls", feature = "ring"))]

use std::{sync::Arc, time::Duration};

use noq::{ClientConfig, Endpoint, ServerConfig};
use rustls::pki_types::PrivatePkcs8KeyDer;

/// Finishing and immediately dropping a public send stream must deliver its
/// queued bytes and EOF, even before the endpoint driver gets another turn.
#[tokio::test]
async fn finished_stream_drop_preserves_buffered_data() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(20), async {
        let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
        let mut roots = rustls::RootCertStore::empty();
        roots.add(certificate.cert.der().clone())?;
        let server_config = ServerConfig::with_single_cert(
            vec![certificate.cert.der().clone()],
            PrivatePkcs8KeyDer::from(certificate.signing_key.serialize_der()).into(),
        )?;
        let server = Endpoint::server(server_config, "127.0.0.1:0".parse()?)?;
        let mut client = Endpoint::client("127.0.0.1:0".parse()?)?;
        client.set_default_client_config(ClientConfig::with_root_certificates(Arc::new(roots))?);
        let (client_connection, server_connection) = tokio::try_join!(
            async { anyhow::Ok(client.connect(server.local_addr()?, "localhost")?.await?) },
            async { anyhow::Ok(server.accept().await.expect("server is listening").await?) },
        )?;
        let payload: Vec<u8> = (0..262_144).map(|index| (index % 251) as u8).collect();
        let mut send = client_connection.open_uni().await?;
        send.write_all(&payload).await?;
        send.finish()?;
        drop(send);
        let mut receive = server_connection.accept_uni().await?;
        let delivered = receive.read_to_end(payload.len()).await?;
        assert_eq!(delivered, payload);
        server_connection.close(0u32.into(), b"all bytes received");
        client.wait_idle().await;
        anyhow::Ok(())
    })
    .await?
}
