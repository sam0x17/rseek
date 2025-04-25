//! Provides a seekable and asynchronous read interface for [`reqwest`] HTTP streams.
//! Continually streams data from a single HTTP request, tearing down and restarting only on seek.

use std::io::{Error as IoError, ErrorKind, Result as IoResult};
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::TryStreamExt;
use reqwest::{RequestBuilder, Response};
use tokio::io::{AsyncRead, AsyncSeek, ReadBuf, SeekFrom};
use tokio_util::io::StreamReader;

type SReader =
    StreamReader<Pin<Box<dyn futures::Stream<Item = Result<Bytes, IoError>> + Send>>, Bytes>;

/// A continuously streaming, seekable HTTP reader.
/// Only on `seek` does it drop the connection and open a new ranged request.
pub struct Seekable<F>
where
    F: Fn() -> RequestBuilder + Send + Sync + 'static,
{
    factory: F,
    /// Known file size, if determined
    pub file_size: Option<u64>,
    /// Current read position
    pub position: u64,

    // In-flight response future when opening connection
    init_fetch: Option<Pin<Box<dyn futures::Future<Output = IoResult<Response>> + Send>>>,
    // Once the response is ready, this yields chunks
    reader: Option<SReader>,
}

// Allow using AsyncReadExt and AsyncSeekExt
impl<F> Unpin for Seekable<F> where F: Fn() -> RequestBuilder + Send + Sync + 'static {}

impl<F> Seekable<F>
where
    F: Fn() -> RequestBuilder + Send + Sync + 'static,
{
    /// Create a new `Seekable`, learn length (if possible), then start an initial full GET.
    pub async fn new(factory: F) -> Self {
        let mut s = Seekable {
            factory,
            file_size: None,
            position: 0,
            init_fetch: None,
            reader: None,
        };
        // try to determine length, ignore failures
        if let Ok(sz) = s.fetch_file_size().await {
            s.file_size = Some(sz);
        }
        // open initial full GET
        s.schedule_fetch(0);
        s
    }

    /// Probe file size via a small range GET.
    pub async fn fetch_file_size(&self) -> IoResult<u64> {
        // Perform a small range request and only accept 206 Partial Content
        let req = (self.factory)().header("Range", "bytes=0-0");
        let resp = req
            .send()
            .await
            .map_err(|e| IoError::new(ErrorKind::Other, e.to_string()))?;
        if resp.status() != reqwest::StatusCode::PARTIAL_CONTENT {
            // Range not supported
            return Err(IoError::new(
                ErrorKind::Unsupported,
                "server does not support range requests",
            ));
        }
        // Parse Content-Range header: bytes 0-0/size
        if let Some(cr) = resp.headers().get("content-range") {
            if let Ok(s) = cr.to_str() {
                if let Some(total) = s.split('/').nth(1) {
                    if let Ok(n) = total.parse::<u64>() {
                        return Ok(n);
                    }
                }
            }
        }
        Err(IoError::new(
            ErrorKind::Other,
            "failed to determine file size",
        ))
    }

    fn schedule_fetch(&mut self, pos: u64) {
        self.reader = None;
        let mut builder = (self.factory)();
        if let Some(sz) = self.file_size {
            let end = sz.saturating_sub(1);
            builder = builder.header("Range", format!("bytes={}-{}", pos, end));
        } else {
            builder = builder.header("Range", format!("bytes={}-", pos));
        }
        let fut = async move {
            builder
                .send()
                .await
                .map_err(|e| IoError::new(ErrorKind::Other, e.to_string()))
        };
        self.init_fetch = Some(Box::pin(fut));
    }
}

impl<F> AsyncRead for Seekable<F>
where
    F: Fn() -> RequestBuilder + Send + Sync + 'static,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<IoResult<()>> {
        // SAFETY: Seekable is Unpin
        let this = unsafe { Pin::get_unchecked_mut(self) };

        // EOF guard
        if let Some(sz) = this.file_size {
            if this.position >= sz {
                return Poll::Ready(Err(IoError::new(ErrorKind::UnexpectedEof, "EOF reached")));
            }
        }

        // Delegate to existing reader
        if let Some(reader) = &mut this.reader {
            let before = buf.filled().len();
            let res = Pin::new(reader).poll_read(cx, buf);
            if let Poll::Ready(Ok(())) = &res {
                this.position += (buf.filled().len() - before) as u64;
            }
            return res;
        }

        // Complete initial fetch
        if let Some(fut) = &mut this.init_fetch {
            match fut.as_mut().poll(cx) {
                Poll::Ready(Ok(resp)) => {
                    let stream = resp
                        .bytes_stream()
                        .map_err(|e| IoError::new(ErrorKind::Other, e.to_string()));
                    this.reader = Some(StreamReader::new(Box::pin(stream)));
                    this.init_fetch = None;
                    // Recurse into reader
                    let pinned = unsafe { Pin::new_unchecked(this) };
                    return AsyncRead::poll_read(pinned, cx, buf);
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }

        Poll::Ready(Err(IoError::new(ErrorKind::UnexpectedEof, "stream closed")))
    }
}

impl<F> AsyncSeek for Seekable<F>
where
    F: Fn() -> RequestBuilder + Send + Sync + 'static,
{
    fn start_seek(self: Pin<&mut Self>, position: SeekFrom) -> IoResult<()> {
        let this = self.get_mut();
        // compute absolute new position
        let new_pos = match position {
            SeekFrom::Start(n) => n,
            SeekFrom::Current(off) => {
                let tmp = this.position as i64 + off;
                if tmp < 0 {
                    return Err(IoError::new(ErrorKind::InvalidInput, "negative seek"));
                }
                tmp as u64
            }
            SeekFrom::End(off) => {
                let sz = this
                    .file_size
                    .ok_or_else(|| IoError::new(ErrorKind::Unsupported, "length unknown"))?;
                let tmp = sz as i64 + off;
                if tmp < 0 {
                    return Err(IoError::new(ErrorKind::InvalidInput, "negative seek"));
                }
                tmp as u64
            }
        };
        this.position = new_pos.min(this.file_size.unwrap_or(u64::MAX));
        this.init_fetch = None;
        this.schedule_fetch(this.position);
        Ok(())
    }

    fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<IoResult<u64>> {
        let this = self.get_mut();
        Poll::Ready(Ok(this.position))
    }
}

#[tokio::test]
async fn test_seekable_http_stream() {
    use reqwest::Client;
    use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};

    let client = Client::new();
    let mut stream = Seekable::new(move || client.get("https://example.com/largefile.bin")).await;
    let mut buf = vec![0u8; 16];

    // Read start
    stream.read_exact(&mut buf).await.unwrap();

    // Seek + read at 1MB
    stream.seek(SeekFrom::Start(1_000_000)).await.unwrap();
    stream.read_exact(&mut buf).await.unwrap();
    let first_1mb = buf.clone();

    // Seek + read at 1.5MB
    stream.seek(SeekFrom::Current(512_000)).await.unwrap();
    stream.read_exact(&mut buf).await.unwrap();

    // Backward seek relative to last read to 1MB
    let back_offset = -(512_000 + buf.len() as i64);
    stream.seek(SeekFrom::Current(back_offset)).await.unwrap();
    let mut back_buf = vec![0u8; 16];
    stream.read_exact(&mut back_buf).await.unwrap();
    assert_eq!(back_buf, first_1mb);
}

#[tokio::test]
async fn test_fetch_file_size_ovh() {
    use reqwest::Client;

    let client = Client::new();
    let stream = Seekable::new(move || client.get("https://proof.ovh.net/files/100Mb.dat")).await;

    let size = Seekable::fetch_file_size(&stream).await.unwrap();

    // Assert that file size is exactly 100MB (104857600 bytes)
    assert_eq!(size, 100 * 1024 * 1024);
}

#[tokio::test]
async fn test_fetch_file_size_of1() {
    use reqwest::Client;

    let client = Client::new();

    let stream =
        Seekable::new(move || client.get("https://files.old-faithful.net/712/epoch-712.car")).await;

    let size = Seekable::fetch_file_size(&stream).await.unwrap();

    assert_eq!(size, 781436491980);
}

#[tokio::test]
async fn test_seek_beyond_eof() {
    use reqwest::Client;
    use tokio::io::AsyncSeekExt;

    let client = Client::new();
    let mut stream =
        Seekable::new(move || client.get("https://proof.ovh.net/files/100Mb.dat")).await;

    let file_size = stream.file_size.unwrap();

    // Seek well beyond EOF
    stream
        .seek(SeekFrom::Start(file_size + 1000))
        .await
        .unwrap();

    // Ensure position is clamped to EOF
    assert_eq!(stream.position, file_size);
}

#[tokio::test]
async fn test_read_at_eof_should_return_eof() {
    use reqwest::Client;
    use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};

    let client = Client::new();
    let mut stream =
        Seekable::new(move || client.get("https://proof.ovh.net/files/100Mb.dat")).await;

    let file_size = stream.file_size.unwrap();

    // Seek to EOF
    stream.seek(SeekFrom::Start(file_size)).await.unwrap();

    let mut buf = vec![0u8; 16];
    let result = stream.read_exact(&mut buf).await;

    // Expect EOF error
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().kind(),
        std::io::ErrorKind::UnexpectedEof
    );
}

#[tokio::test]
async fn test_fetch_near_eof_should_only_fetch_remaining_bytes() {
    use reqwest::Client;
    use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};

    let client = Client::new();
    let mut stream =
        Seekable::new(move || client.get("https://proof.ovh.net/files/100Mb.dat")).await;

    let file_size = stream.file_size.unwrap();

    // Seek close to EOF
    stream.seek(SeekFrom::Start(file_size - 10)).await.unwrap();

    let mut buf = vec![0u8; 16]; // Try to read past EOF
    let result = stream.read_exact(&mut buf).await;

    // Expect an EOF error because there's not enough data to fill the buffer
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().kind(),
        std::io::ErrorKind::UnexpectedEof
    );
}

#[tokio::test]
async fn test_seek_before_start_should_error() {
    use reqwest::Client;
    use tokio::io::AsyncSeekExt;

    let client = Client::new();
    let mut stream =
        Seekable::new(move || client.get("https://proof.ovh.net/files/100Mb.dat")).await;

    // Seek to a negative position
    let result = stream.seek(SeekFrom::Current(-1_000_000_000)).await;

    // Should return an error
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::InvalidInput);
}

#[tokio::test]
async fn test_seek_to_end_of_enormous_file() {
    use reqwest::Client;
    use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};

    let client = Client::new();

    let mut stream =
        Seekable::new(move || client.get("https://files.old-faithful.net/725/epoch-725.car")).await;

    let mut buf = vec![0u8; 16]; // Read 16 bytes

    // Read first 16 bytes at the start
    stream.read_exact(&mut buf).await.unwrap();
    println!("First 16 bytes: {:?}", buf);

    // Seek forward by 1MB and read again
    stream.seek(SeekFrom::Start(1_000_000)).await.unwrap();
    stream.read_exact(&mut buf).await.unwrap();
    println!("Bytes after seeking to 1MB: {:?}", buf);

    // Seek to the end of the file minus 16 bytes
    stream.seek(SeekFrom::End(-16)).await.unwrap();
    stream.read_exact(&mut buf).await.unwrap();
    println!("Bytes after seeking to 16 bytes before EOF: {:?}", buf);
    assert_eq!(
        buf,
        vec![
            22, 247, 241, 176, 61, 255, 51, 33, 66, 108, 17, 240, 234, 176, 48, 222
        ]
    );
    stream.seek(SeekFrom::End(-16)).await.unwrap();
    stream.read_exact(&mut buf).await.unwrap();
    assert_eq!(
        buf,
        vec![
            22, 247, 241, 176, 61, 255, 51, 33, 66, 108, 17, 240, 234, 176, 48, 222
        ]
    );
}

#[tokio::test]
async fn test_long_read() {
    use reqwest::Client;
    use tokio::io::AsyncReadExt;

    let client = Client::new();

    let mut stream =
        Seekable::new(move || client.get("https://files.old-faithful.net/725/epoch-725.car")).await;

    let mut buf = vec![0u8; 400 * 1024 * 1024];
    stream.read_exact(&mut buf).await.unwrap();
}
