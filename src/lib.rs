use std::io::{Error as IoError, ErrorKind, Result as IoResult};
use std::ops::Range;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{Buf, Bytes};
use futures::future::BoxFuture;
use reqwest::RequestBuilder;
use tokio::io::{AsyncRead, AsyncSeek, SeekFrom};

pub struct Seekable {
    request_template: RequestBuilder, // Store the original request to clone from
    position: u64,
    buffer: Bytes,
    pending_fetch: Option<BoxFuture<'static, IoResult<Bytes>>>, // Fetching in progress
}

impl Seekable {
    pub fn new(request: RequestBuilder) -> IoResult<Self> {
        let request_template = request
            .try_clone()
            .ok_or_else(|| IoError::new(ErrorKind::Other, "Failed to clone request"))?;

        Ok(Self {
            request_template,
            position: 0,
            buffer: Bytes::new(),
            pending_fetch: None,
        })
    }

    fn start_fetch(&mut self, range: Range<u64>) {
        let request = self
            .request_template
            .try_clone()
            .ok_or_else(|| IoError::new(ErrorKind::Other, "Failed to clone request template"));

        let fetch_future = async move {
            let request = request?;
            let response = request
                .header("Range", format!("bytes={}-{}", range.start, range.end - 1))
                .send()
                .await
                .map_err(|e| IoError::new(ErrorKind::Other, e.to_string()))?;

            response
                .bytes()
                .await
                .map_err(|e| IoError::new(ErrorKind::Other, e.to_string()))
        };

        self.pending_fetch = Some(Box::pin(fetch_future));
    }

    /// Fetches the total file size by making a request with `Range: bytes=0-0`
    async fn fetch_file_size(request: &RequestBuilder) -> IoResult<u64> {
        let request = request
            .try_clone()
            .ok_or_else(|| IoError::new(ErrorKind::Other, "Failed to clone request"))?;

        let response = request
            .header("Range", "bytes=0-0")
            .send()
            .await
            .map_err(|e| IoError::new(ErrorKind::Other, e.to_string()))?;

        // Ensure the response is successful (200 OK or 206 Partial Content)
        if !response.status().is_success() {
            return Err(IoError::new(
                ErrorKind::Other,
                format!("Unexpected response status: {}", response.status()),
            ));
        }

        // Try `Content-Range` first
        if let Some(content_range) = response.headers().get("content-range") {
            let content_range = content_range.to_str().unwrap_or("");
            if let Some(size_str) = content_range.split('/').nth(1) {
                if let Ok(size) = size_str.parse::<u64>() {
                    return Ok(size);
                }
            }
        }

        // Fallback to `Content-Length`
        if let Some(content_length) = response.headers().get("Content-Length") {
            if let Ok(size) = content_length.to_str().unwrap_or("").parse::<u64>() {
                return Ok(size);
            }
        }

        Err(IoError::new(
            ErrorKind::Other,
            "Failed to determine file size",
        ))
    }
}

impl AsyncRead for Seekable {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<IoResult<()>> {
        // If buffer is empty, start fetching more data
        if self.buffer.is_empty() {
            if self.pending_fetch.is_none() {
                let range = self.position..self.position + 8192;
                self.start_fetch(range);
            }

            // Poll future if it's still pending
            if let Some(future) = &mut self.pending_fetch {
                match Pin::new(future).poll(cx) {
                    Poll::Ready(Ok(bytes)) => {
                        self.buffer = bytes;
                        self.pending_fetch = None;
                    }
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Pending => return Poll::Pending,
                }
            }
        }

        if self.buffer.is_empty() {
            return Poll::Ready(Err(IoError::new(ErrorKind::UnexpectedEof, "EOF reached")));
        }

        let to_copy = buf.remaining().min(self.buffer.len());
        buf.put_slice(&self.buffer[..to_copy]);
        self.buffer.advance(to_copy);
        self.position += to_copy as u64;

        Poll::Ready(Ok(()))
    }
}

impl AsyncSeek for Seekable {
    fn start_seek(mut self: Pin<&mut Self>, position: SeekFrom) -> IoResult<()> {
        self.position = match position {
            SeekFrom::Start(pos) => pos,
            SeekFrom::End(_) => {
                return Err(IoError::new(
                    ErrorKind::Unsupported,
                    "Seek from end not supported",
                ));
            }
            SeekFrom::Current(offset) => {
                let new_pos = self.position as i64 + offset;
                if new_pos < 0 {
                    return Err(IoError::new(
                        ErrorKind::InvalidInput,
                        "Negative seek position",
                    ));
                }
                new_pos as u64
            }
        };

        // Invalidate buffer and fetch new range
        self.buffer = Bytes::new();
        self.pending_fetch = None;

        Ok(())
    }

    fn poll_complete(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<u64>> {
        if let Some(future) = &mut self.pending_fetch {
            match Pin::new(future).poll(cx) {
                Poll::Ready(Ok(bytes)) => {
                    self.buffer = bytes;
                    self.pending_fetch = None;
                    return Poll::Ready(Ok(self.position));
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }

        Poll::Ready(Ok(self.position))
    }
}

#[tokio::test]
async fn test_seekable_http_stream() -> std::io::Result<()> {
    use reqwest::Client;
    use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};

    let client = Client::new();
    let request = client.get("https://example.com/largefile.bin");

    let mut stream = Seekable::new(request)?;

    let mut buf = vec![0u8; 16]; // Read 16 bytes

    // Read first 16 bytes at the start
    stream.read_exact(&mut buf).await?;
    println!("First 16 bytes: {:?}", buf);

    // Seek forward by 1MB and read again
    stream.seek(SeekFrom::Start(1_000_000)).await?;
    stream.read_exact(&mut buf).await?;
    println!("Bytes after seeking to 1MB: {:?}", buf);

    // Seek forward again by another 512KB
    stream.seek(SeekFrom::Current(512_000)).await?;
    stream.read_exact(&mut buf).await?;
    println!("Bytes after seeking to 1.5MB: {:?}", buf);

    // Seek backward by 512KB (back to 1MB mark)
    stream.seek(SeekFrom::Current(-512_000)).await?;
    let mut buf_after_backseek = vec![0u8; 16];
    stream.read_exact(&mut buf_after_backseek).await?;

    // Verify that seeking back returns the same bytes as the first seek to 1MB
    assert_eq!(
        buf, buf_after_backseek,
        "Bytes after seeking back should match original read"
    );

    println!("Seek test passed!");

    Ok(())
}

#[tokio::test]
async fn test_fetch_file_size() {
    use reqwest::Client;

    let client = Client::new();
    let request = client.get("https://proof.ovh.net/files/100Mb.dat");

    let size = Seekable::fetch_file_size(&request).await.unwrap();

    // Assert that file size is exactly 100MB (104857600 bytes)
    assert_eq!(size, 100 * 1024 * 1024);
}

#[tokio::test]
async fn test_fetch_file_size_failure() {
    use reqwest::Client;

    let client = Client::new();
    let request = client.get("https://proof.ovh.net/files/nonexistent.dat");

    Seekable::fetch_file_size(&request).await.unwrap_err();
}
