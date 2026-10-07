use std::io::ErrorKind;
use std::time::Duration;

use rseek::Seekable;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio::time::timeout;

const DEADLINE: Duration = Duration::from_secs(10);

fn file_bytes(start: u64, len: usize) -> Vec<u8> {
    (start..start + len as u64)
        .map(|offset| (offset.wrapping_mul(31) ^ (offset >> 8)) as u8)
        .collect()
}

struct RangeServer {
    url: String,
    task: JoinHandle<()>,
}

impl RangeServer {
    // The first request is always the constructor's one-byte size probe.
    async fn start(size: u64, reads: &[(u64, u64)]) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/data", listener.local_addr().unwrap());
        let ranges: Vec<_> = std::iter::once((0, 0))
            .chain(reads.iter().copied())
            .collect();
        let task = tokio::spawn(async move {
            timeout(DEADLINE, async move {
                for (start, end) in ranges {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    let mut request = Vec::new();
                    while !request.ends_with(b"\r\n\r\n") {
                        assert!(request.len() < 8192, "oversized request headers");
                        request.push(socket.read_u8().await.unwrap());
                    }
                    let request = String::from_utf8(request).unwrap();
                    let range = request.lines().find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("range").then(|| value.trim())
                    });
                    assert_eq!(range, Some(format!("bytes={start}-{end}").as_str()));
                    let len = end - start + 1;
                    assert!(len <= 128 * 1024, "fixture must not send a huge file");
                    let headers = format!(
                        "HTTP/1.1 206 Partial Content\r\n\
                         Content-Range: bytes {start}-{end}/{size}\r\n\
                         Content-Length: {len}\r\n\
                         Connection: close\r\n\r\n"
                    );
                    socket.write_all(headers.as_bytes()).await.unwrap();
                    for chunk in file_bytes(start, len as usize).chunks(1024) {
                        if let Err(error) = socket.write_all(chunk).await {
                            // A seek intentionally abandons the previous response.
                            assert!(matches!(
                                error.kind(),
                                ErrorKind::BrokenPipe | ErrorKind::ConnectionReset
                            ));
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                }
            })
            .await
            .expect("range server timed out");
        });
        Self { url, task }
    }

    async fn reader(
        &self,
    ) -> Seekable<impl Fn() -> reqwest::RequestBuilder + Send + Sync + 'static + use<>> {
        let client = reqwest::Client::builder()
            .no_proxy()
            .http1_only()
            .timeout(DEADLINE)
            .build()
            .unwrap();
        let url = self.url.clone();
        Seekable::new(move || client.get(&url)).await
    }

    async fn finish(mut self) {
        timeout(DEADLINE, &mut self.task)
            .await
            .expect("range server did not finish")
            .expect("range server failed");
    }
}

impl Drop for RangeServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[tokio::test]
async fn probes_small_and_large_file_sizes() {
    for size in [4096, 781_436_491_980] {
        let server = RangeServer::start(size, &[(0, 0)]).await;
        let stream = server.reader().await;
        assert_eq!(stream.file_size, Some(size));
        assert_eq!(stream.fetch_file_size().await.unwrap(), size);
        server.finish().await;
    }
}

#[tokio::test]
async fn reads_correct_bytes_after_absolute_and_relative_seeks() {
    let size = 4096;
    let server = RangeServer::start(
        size,
        &[
            (0, size - 1),
            (1024, size - 1),
            (1568, size - 1),
            (1024, size - 1),
        ],
    )
    .await;
    let mut stream = server.reader().await;
    let mut buf = [0; 32];
    stream.read_exact(&mut buf).await.unwrap();
    assert_eq!(buf.as_slice(), file_bytes(0, 32));
    assert_eq!(stream.position, 32);

    assert_eq!(stream.seek(SeekFrom::Start(1024)).await.unwrap(), 1024);
    stream.read_exact(&mut buf).await.unwrap();
    assert_eq!(buf.as_slice(), file_bytes(1024, 32));

    assert_eq!(stream.seek(SeekFrom::Current(512)).await.unwrap(), 1568);
    stream.read_exact(&mut buf).await.unwrap();
    assert_eq!(buf.as_slice(), file_bytes(1568, 32));

    assert_eq!(stream.seek(SeekFrom::Current(-576)).await.unwrap(), 1024);
    stream.read_exact(&mut buf).await.unwrap();
    assert_eq!(buf.as_slice(), file_bytes(1024, 32));
    assert_eq!(stream.position, 1056);
    server.finish().await;
}

#[tokio::test]
async fn rejects_negative_seeks_and_clamps_beyond_eof() {
    let size = 4096;
    let server = RangeServer::start(size, &[]).await;
    let mut stream = server.reader().await;
    for seek in [SeekFrom::Current(-1), SeekFrom::End(-(size as i64) - 1)] {
        let error = stream.seek(seek).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert_eq!(stream.position, 0);
    }
    assert_eq!(
        stream.seek(SeekFrom::Start(size + 1000)).await.unwrap(),
        size
    );
    assert_eq!(stream.position, size);
    server.finish().await;
}

#[tokio::test]
async fn read_at_eof_returns_unexpected_eof() {
    let size = 4096;
    let server = RangeServer::start(size, &[]).await;
    let mut stream = server.reader().await;
    stream.seek(SeekFrom::End(0)).await.unwrap();
    let mut buf = [0xa5; 16];
    let error = stream.read_exact(&mut buf).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::UnexpectedEof);
    assert_eq!(buf, [0xa5; 16]);
    assert_eq!(stream.position, size);
    server.finish().await;
}

#[tokio::test]
async fn short_tail_only_reads_remaining_bytes() {
    let size = 4096;
    let server = RangeServer::start(size, &[(size - 10, size - 1)]).await;
    let mut stream = server.reader().await;
    stream.seek(SeekFrom::End(-10)).await.unwrap();
    let mut buf = [0xa5; 16];
    let error = stream.read_exact(&mut buf).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::UnexpectedEof);
    assert_eq!(&buf[..10], file_bytes(size - 10, 10));
    assert_eq!(&buf[10..], &[0xa5; 6]);
    assert_eq!(stream.position, size);
    server.finish().await;
}

#[tokio::test]
async fn seeks_to_the_end_of_a_large_virtual_file() {
    let size = 781_436_491_980;
    let server = RangeServer::start(size, &[(size - 16, size - 1); 2]).await;
    let mut stream = server.reader().await;
    for _ in 0..2 {
        assert_eq!(stream.seek(SeekFrom::End(-16)).await.unwrap(), size - 16);
        let mut buf = [0; 16];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf.as_slice(), file_bytes(size - 16, 16));
        assert_eq!(stream.position, size);
    }
    server.finish().await;
}

#[tokio::test]
async fn continuous_reads_use_one_response() {
    let size = 64 * 1024 + 37;
    let server = RangeServer::start(size, &[(0, size - 1)]).await;
    let mut stream = server.reader().await;
    let mut buf = [0; 4096];
    let mut position = 0;
    while position < size {
        let len = (size - position).min(buf.len() as u64) as usize;
        stream.read_exact(&mut buf[..len]).await.unwrap();
        assert_eq!(&buf[..len], file_bytes(position, len));
        position += len as u64;
        assert_eq!(stream.position, position);
    }
    server.finish().await;
}
