use std::sync::Arc;
use std::time::Duration;

use axum::{body::Bytes, extract::State, http::HeaderMap, routing::post, Router};
use tokio::sync::Mutex;

type CapturedUpload = Arc<Mutex<Option<(String, Vec<u8>)>>>;

async fn capture_upload(State(captured): State<CapturedUpload>, headers: HeaderMap, body: Bytes) -> &'static str {
    let content_type = headers[axum::http::header::CONTENT_TYPE].to_str().unwrap().to_owned();
    *captured.lock().await = Some((content_type, body.to_vec()));
    "accepted"
}

#[tokio::test]
async fn multipart_is_transmitted_to_a_real_loopback_http_server() {
    let captured: CapturedUpload = Arc::new(Mutex::new(None));
    let app = Router::new().route("/upload", post(capture_upload)).with_state(captured.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap(); });
    let client = reqwest::Client::builder().no_proxy().timeout(Duration::from_secs(5)).build().unwrap();
    let response = client.post(format!("http://{address}/upload"))
        .multipart(reqwest::multipart::Form::new().text("field", "synthetic payload"))
        .send().await;
    task.abort();
    let response = response.unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let guard = captured.lock().await;
    let (content_type, body) = guard.as_ref().unwrap();
    let boundary = content_type.strip_prefix("multipart/form-data; boundary=").unwrap();
    let body = std::str::from_utf8(body).unwrap();
    assert!(body.contains(&format!("--{boundary}\r\n")));
    assert!(body.contains("name=\"field\""));
    assert!(body.contains("\r\n\r\nsynthetic payload\r\n"));
    assert!(body.ends_with(&format!("--{boundary}--\r\n")));
}

#[cfg(target_os = "linux")]
mod tls {
    use super::*;
    use std::process::{Child, Command, Stdio};

    struct Server(Child);
    impl Drop for Server {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    #[tokio::test]
    async fn trusted_certificate_succeeds_but_unknown_root_and_wrong_name_fail() {
        let dir = tempfile::tempdir().unwrap();
        let key = dir.path().join("ephemeral.key");
        let cert = dir.path().join("ephemeral.crt");
        // Per-execution test key material is removed with TempDir.
        let generated = Command::new("openssl").args([
            "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
            "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost",
        ]).arg("-keyout").arg(&key).arg("-out").arg(&cert)
            .stdout(Stdio::null()).stderr(Stdio::null()).status().unwrap();
        assert!(generated.success(), "test certificate creation failed");
        let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = reservation.local_addr().unwrap();
        drop(reservation);
        let mut server = Server(Command::new("openssl").args(["s_server", "-accept"])
            .arg(address.to_string()).arg("-cert").arg(&cert).arg("-key").arg(&key)
            .args(["-www", "-quiet"]).stdin(Stdio::null()).stdout(Stdio::null())
            .stderr(Stdio::null()).spawn().unwrap());
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                assert!(server.0.try_wait().unwrap().is_none(), "test TLS server exited");
                if tokio::net::TcpStream::connect(address).await.is_ok() { break; }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }).await.unwrap();
        let certificate = reqwest::Certificate::from_pem(&std::fs::read(cert).unwrap()).unwrap();
        let client = |trusted: bool| {
            let builder = reqwest::Client::builder().no_proxy().use_rustls_tls()
                .tls_built_in_root_certs(false).timeout(Duration::from_secs(5))
                .resolve("localhost", address).resolve("wrong.example.invalid", address);
            if trusted { builder.add_root_certificate(certificate.clone()).build().unwrap() }
            else { builder.build().unwrap() }
        };
        let url = format!("https://localhost:{}/", address.port());
        let unknown = client(false).get(&url).send().await.unwrap_err();
        assert!(unknown.is_connect() && !unknown.is_timeout());
        let wrong_name = client(true).get(format!("https://wrong.example.invalid:{}/", address.port()))
            .send().await.unwrap_err();
        assert!(wrong_name.is_connect() && !wrong_name.is_timeout());
        // A successful control after the negatives rules out a dead server.
        let response = client(true).get(url).send().await.unwrap();
        assert!(response.status().is_success());
        assert!(!response.bytes().await.unwrap().is_empty());
    }
}
