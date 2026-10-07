use std::{io::ErrorKind, time::Duration};

use reqwest::Client;
use rseek::Seekable;
use tokio::{
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom},
    net::TcpListener,
    task::JoinHandle,
    time::timeout,
};

struct HttpServer {
    url: String,
    task: Option<JoinHandle<()>>,
}

impl HttpServer {
    async fn start(responses: Vec<(&'static str, String)>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/archive.car", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            timeout(Duration::from_secs(10), async move {
                for (range, response) in responses {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    let mut request = Vec::new();
                    while !request.ends_with(b"\r\n\r\n") {
                        assert!(request.len() < 16_384, "request headers too large");
                        request.push(socket.read_u8().await.unwrap());
                    }
                    let request = String::from_utf8(request).unwrap();
                    assert_eq!(request.lines().next(), Some("GET /archive.car HTTP/1.1"));
                    assert!(
                        request.lines().any(|line| {
                            line.split_once(':').is_some_and(|(name, value)| {
                                name.eq_ignore_ascii_case("range") && value.trim() == range
                            })
                        }),
                        "expected Range: {range}, received {request:?}"
                    );
                    socket.write_all(response.as_bytes()).await.unwrap();
                }
            })
            .await
            .expect("HTTP fixture timed out");
        });
        Self {
            url,
            task: Some(task),
        }
    }

    fn factory(&self) -> impl Fn() -> reqwest::RequestBuilder + Send + Sync + 'static + use<> {
        let client = Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap();
        let url = self.url.clone();
        move || client.get(&url)
    }

    async fn finish(mut self) {
        self.task.take().unwrap().await.unwrap();
    }
}

impl Drop for HttpServer {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

fn partial(content_range: &str, body: &str) -> String {
    format!(
        "HTTP/1.1 206 Partial Content\r\nContent-Range: {content_range}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}

fn http_error(status: u16) -> String {
    let reason = match status {
        429 => "Too Many Requests",
        503 => "Service Unavailable",
        _ => unreachable!(),
    };
    let body = "This is an HTTP error, not archive data.";
    format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}

fn assert_http_error(error: &std::io::Error, status: u16) {
    assert_eq!(error.kind(), ErrorKind::Other);
    let source = error
        .get_ref()
        .and_then(|source| source.downcast_ref::<reqwest::Error>())
        .expect("HTTP errors should retain their reqwest source");
    assert_eq!(source.status().unwrap().as_u16(), status);
    assert!(
        error.to_string().contains(&status.to_string()),
        "expected HTTP status {status} in {error:?}"
    );
}

#[tokio::test]
async fn initial_http_error_does_not_return_body_and_seek_can_retry() {
    for status in [429, 503] {
        let server = HttpServer::start(vec![
            ("bytes=0-0", partial("bytes 0-0/6", "a")),
            ("bytes=0-5", http_error(status)),
            ("bytes=0-5", partial("bytes 0-5/6", "abcdef")),
        ])
        .await;
        let mut stream = Seekable::new(server.factory()).await;
        assert_eq!(stream.file_size, Some(6));

        let mut buffer = [0xa5; 6];
        let error = stream.read(&mut buffer).await.unwrap_err();
        assert_http_error(&error, status);
        assert_eq!(buffer, [0xa5; 6]);
        assert_eq!(stream.position, 0);

        // Polling again must not poll a completed response future and panic.
        let error = stream.read(&mut buffer).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::UnexpectedEof);
        assert_eq!(buffer, [0xa5; 6]);
        assert_eq!(stream.position, 0);

        stream.seek(SeekFrom::Start(0)).await.unwrap();
        stream.read_exact(&mut buffer).await.unwrap();
        assert_eq!(&buffer, b"abcdef");
        assert_eq!(stream.position, 6);
        server.finish().await;
    }
}

#[tokio::test]
async fn http_error_after_seek_does_not_return_body_or_advance_position() {
    for status in [429, 503] {
        let server = HttpServer::start(vec![
            ("bytes=0-0", partial("bytes 0-0/10", "a")),
            ("bytes=0-9", partial("bytes 0-9/10", "abcdefghij")),
            ("bytes=5-9", http_error(status)),
        ])
        .await;
        let mut stream = Seekable::new(server.factory()).await;
        let mut buffer = [0; 2];
        stream.read_exact(&mut buffer).await.unwrap();
        assert_eq!(&buffer, b"ab");
        assert_eq!(stream.position, 2);

        stream.seek(SeekFrom::Start(5)).await.unwrap();
        let error = stream.read(&mut buffer).await.unwrap_err();
        assert_http_error(&error, status);
        assert_eq!(&buffer, b"ab");
        assert_eq!(stream.position, 5);
        server.finish().await;
    }
}

#[tokio::test]
async fn failed_size_probe_preserves_status_and_unknown_size_read_rejects_error() {
    for status in [429, 503] {
        let server = HttpServer::start(vec![
            ("bytes=0-0", http_error(status)),
            ("bytes=0-0", http_error(status)),
            ("bytes=0-", http_error(status)),
        ])
        .await;
        let mut stream = Seekable::new(server.factory()).await;
        assert_eq!(stream.file_size, None);

        let error = stream.fetch_file_size().await.unwrap_err();
        assert_http_error(&error, status);

        let mut buffer = [0xa5; 8];
        let error = stream.read(&mut buffer).await.unwrap_err();
        assert_http_error(&error, status);
        assert_eq!(buffer, [0xa5; 8]);
        assert_eq!(stream.position, 0);
        server.finish().await;
    }
}

#[tokio::test]
async fn successful_unknown_length_response_remains_readable() {
    let response = "HTTP/1.1 200 OK\r\nConnection: close\r\n\r\narchive data";
    let server = HttpServer::start(vec![
        ("bytes=0-0", response.to_owned()),
        ("bytes=0-", response.to_owned()),
    ])
    .await;
    let mut stream = Seekable::new(server.factory()).await;
    assert_eq!(stream.file_size, None);

    let mut buffer = Vec::new();
    stream.read_to_end(&mut buffer).await.unwrap();
    assert_eq!(buffer, b"archive data");
    assert_eq!(stream.position, buffer.len() as u64);
    server.finish().await;
}
