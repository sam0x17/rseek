//! Provides a seekable and asynchronous read interface for [`reqwest`] HTTP streams. This is
//! useful for handling large files over HTTP where random access is required. This
//! implementation assumes the server supports HTTP range requests. Servers that do not support
//! range requests are still usable, however certain seeking features will be unavailable.
//!
//! If the file size cannot be determined, the implementation will attempt to fetch data
//! without bounds, relying on the server to handle the request appropriately.

use std::io::{Error as IoError, ErrorKind, Result as IoResult};
use std::ops::Range;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{Buf, Bytes};
use futures::future::BoxFuture;
use reqwest::RequestBuilder;
use tokio::io::{AsyncRead, AsyncSeek, SeekFrom};

/// Provides a seekable and asynchronous read interface for [`reqwest`] HTTP streams.
/// This is useful for handling large files over HTTP where random access is required.
///
/// ## Type Parameters
/// - `F`: A closure type that generates a [`RequestBuilder`] for HTTP requests.
///
/// ## Methods
/// - `new`: Creates a new [`Seekable`] instance and fetches the file size if available.
///
/// ## Traits Implemented
/// - `AsyncRead`: Allows asynchronous reading of data from the HTTP stream.
/// - `AsyncSeek`: Allows seeking to specific positions in the HTTP stream.
///
/// ## Example
/// ```
/// use reqwest::Client;
/// use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};
///
/// #[tokio::main]
/// async fn main() {
///     use rseek::Seekable;
///     let client = Client::new();
///     let mut stream = Seekable::new(move || client.get("https://example.com/largefile.bin")).await;
///
///     let mut buf = vec![0u8; 16];
///     stream.read_exact(&mut buf).await.unwrap();
///     println!("First 16 bytes: {:?}", buf);
///
///     stream.seek(SeekFrom::Start(1_000_000)).await.unwrap();
///     stream.read_exact(&mut buf).await.unwrap();
///     println!("Bytes after seeking to 1MB: {:?}", buf);
/// }
/// ```
///
/// ## Notes
/// - This implementation assumes the server supports HTTP range requests. Servers that do not
///   support range requests are still usable, however certain seeking features will be
///   unavailable.
/// - If the file size cannot be determined, the implementation will attempt to fetch data
///   without bounds, relying on the server to handle the request appropriately.
///
/// ## Errors
/// - Returns `UnexpectedEof` if attempting to read past the end of the file.
/// - Returns `InvalidInput` if seeking to a negative position.
/// - Returns `Unsupported` if seeking from the end when the file size is unknown.
pub struct Seekable<F>
where
    F: Fn() -> RequestBuilder + Send + Sync + 'static,
{
    request_builder_factory: F, // Closure to generate RequestBuilder
    file_size: Option<u64>,     // Store the file size
    position: u64,
    buffer: Bytes,
    buffer_size: u64,
    pending_fetch: Option<BoxFuture<'static, IoResult<Bytes>>>,
}

impl<F> Seekable<F>
where
    F: Fn() -> RequestBuilder + Send + Sync + 'static,
{
    /// Creates a new [`Seekable`] instance and fetches the file size if available.
    ///
    /// ## Parameters
    /// - `request_builder_factory`: A closure that generates a [`RequestBuilder`] for HTTP
    ///    requests. This closure is called whenever a new HTTP request is required. The closure
    ///    should return a [`RequestBuilder`] that is ready to be sent.
    ///
    /// ## Returns
    /// A new [`Seekable`] instance.
    pub async fn new(request_builder_factory: F) -> Self {
        let mut instance = Self {
            request_builder_factory,
            file_size: None,
            position: 0,
            buffer: Bytes::new(),
            buffer_size: 0,
            pending_fetch: None,
        };

        // Fetch and store file size
        instance.file_size = match instance.fetch_file_size().await {
            Ok(size) => {
                instance.buffer_size = ideal_buffer_size(size);
                Some(size)
            }
            _ => {
                instance.buffer_size = 64 * 1024; // 64 KB
                None
            }
        };

        instance
    }

    /// Allows overriding the intelligently-calculated buffer size with a custom value. See [`Self::new`].
    pub async fn new_with_buffer_size(request_builder_factory: F, buffer_size: u64) -> Self {
        let mut instance = Self {
            request_builder_factory,
            file_size: None,
            position: 0,
            buffer: Bytes::new(),
            buffer_size,
            pending_fetch: None,
        };
        instance.file_size = instance.fetch_file_size().await.ok();
        instance
    }

    fn start_fetch(&mut self, range: Range<u64>) {
        let request = if let Some(file_size) = self.file_size {
            if range.start >= file_size {
                self.pending_fetch = Some(Box::pin(async { Ok(Bytes::new()) }));
                return;
            }

            let end = range.end.min(file_size);
            if end <= range.start {
                return;
            }

            (self.request_builder_factory)()
                .header("Range", format!("bytes={}-{}", range.start, end - 1))
        } else {
            // No file size known, use open-ended range request
            (self.request_builder_factory)().header("Range", format!("bytes={}-", range.start))
        };

        let fetch_future = async move {
            let response = request
                .send()
                .await
                .map_err(|e| IoError::new(ErrorKind::Other, e.to_string()))?;

            let body = response
                .bytes()
                .await
                .map_err(|e| IoError::new(ErrorKind::Other, e.to_string()))?;

            if body.is_empty() {
                return Ok(Bytes::new());
            }

            Ok(body)
        };

        self.pending_fetch = Some(Box::pin(fetch_future));
    }

    async fn fetch_file_size(&self) -> IoResult<u64> {
        let request = (self.request_builder_factory)().header("Range", "bytes=0-0");

        let response = request
            .send()
            .await
            .map_err(|e| IoError::new(ErrorKind::Other, e.to_string()))?;

        if !response.status().is_success() {
            return Err(IoError::new(
                ErrorKind::Other,
                format!("Unexpected response status: {}", response.status()),
            ));
        }

        if let Some(content_range) = response.headers().get("content-range") {
            let content_range = content_range.to_str().unwrap_or("");
            if let Some(size_str) = content_range.split('/').nth(1) {
                if let Ok(size) = size_str.parse::<u64>() {
                    return Ok(size);
                }
            }
        }

        if let Some(content_length) = response.headers().get("content-length") {
            if let Ok(size) = content_length.to_str().unwrap_or("").parse::<u64>() {
                return Ok(size);
            }
        }

        Err(IoError::new(
            ErrorKind::Other,
            "Failed to determine file size",
        ))
    }

    /// Returns the total size of the file being downloaded, if known.
    pub fn file_size(&self) -> Option<u64> {
        self.file_size
    }
}

impl<F> AsyncRead for Seekable<F>
where
    F: Fn() -> RequestBuilder + Send + Sync + 'static + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<IoResult<()>> {
        let this = self.get_mut();

        // If we're past EOF, return EOF error
        if let Some(file_size) = this.file_size {
            if this.position >= file_size {
                return Poll::Ready(Err(IoError::new(ErrorKind::UnexpectedEof, "EOF reached")));
            }
        }

        if this.buffer.is_empty() {
            if this.pending_fetch.is_none() {
                let fetch_size = this.buffer_size;
                let end = this.position + fetch_size;

                this.start_fetch(this.position..end);
            }

            if let Some(future) = &mut this.pending_fetch {
                match Pin::new(future).poll(cx) {
                    Poll::Ready(Ok(bytes)) => {
                        this.buffer = bytes;
                        this.pending_fetch = None;
                    }
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Pending => return Poll::Pending,
                }
            }
        }

        if let Some(file_size) = this.file_size {
            if this.buffer.is_empty() && file_size > 0 {
                return Poll::Ready(Err(IoError::new(ErrorKind::UnexpectedEof, "EOF reached")));
            }
        }

        let to_copy = buf.remaining().min(this.buffer.len());
        buf.put_slice(&this.buffer[..to_copy]);
        this.buffer.advance(to_copy);
        this.position += to_copy as u64;

        Poll::Ready(Ok(()))
    }
}

impl<F> AsyncSeek for Seekable<F>
where
    F: Fn() -> RequestBuilder + Send + Sync + 'static + Unpin,
{
    fn start_seek(self: Pin<&mut Self>, position: SeekFrom) -> IoResult<()> {
        let this = self.get_mut();

        this.position = match position {
            SeekFrom::Start(pos) => pos,
            SeekFrom::End(offset) => {
                let file_size = this.file_size.ok_or_else(|| {
                    IoError::new(ErrorKind::Unsupported, "File size not available")
                })?;

                let new_pos = file_size as i64 + offset;
                if new_pos < 0 {
                    return Err(IoError::new(
                        ErrorKind::InvalidInput,
                        "Negative seek position",
                    ));
                }
                new_pos as u64
            }
            SeekFrom::Current(offset) => {
                let new_pos = this.position as i64 + offset;
                if new_pos < 0 {
                    return Err(IoError::new(
                        ErrorKind::InvalidInput,
                        "Negative seek position",
                    ));
                }
                new_pos as u64
            }
        };

        if let Some(file_size) = this.file_size {
            this.position = this.position.min(file_size);
        }

        this.buffer = Bytes::new();
        this.pending_fetch = None;

        Ok(())
    }

    fn poll_complete(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<u64>> {
        let this = self.get_mut();

        if let Some(future) = &mut this.pending_fetch {
            match Pin::new(future).poll(cx) {
                Poll::Ready(Ok(bytes)) => {
                    this.buffer = bytes;
                    this.pending_fetch = None;
                    return Poll::Ready(Ok(this.position));
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }

        Poll::Ready(Ok(this.position))
    }
}

/// Default buffer size for reading data from the HTTP stream.
pub const fn ideal_buffer_size(file_size: u64) -> u64 {
    // 1 MB
    if file_size < 1024 * 1024 {
        // 8 KB
        return 8192;
    }
    // 10 MB
    if file_size < 1024 * 1024 * 10 {
        // 64 KB
        return 65536;
    }
    // 1 GB
    if file_size < 1024 * 1024 * 1024 {
        // 256 KB
        return 262144;
    }
    // 512 KB
    524288
}

#[tokio::test]
async fn test_seekable_http_stream() {
    use reqwest::Client;
    use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};

    let client = Client::new();

    let mut stream = Seekable::new(move || client.get("https://example.com/largefile.bin")).await;

    let mut buf = vec![0u8; 16]; // Read 16 bytes

    // Read first 16 bytes at the start
    stream.read_exact(&mut buf).await.unwrap();
    println!("First 16 bytes: {:?}", buf);

    // Seek forward by 1MB and read again
    stream.seek(SeekFrom::Start(1_000_000)).await.unwrap();
    stream.read_exact(&mut buf).await.unwrap();
    println!("Bytes after seeking to 1MB: {:?}", buf);

    // Seek forward again by another 512KB
    stream.seek(SeekFrom::Current(512_000)).await.unwrap();
    stream.read_exact(&mut buf).await.unwrap();
    println!("Bytes after seeking to 1.5MB: {:?}", buf);

    // Seek backward by 512KB (back to 1MB mark)
    stream.seek(SeekFrom::Current(-512_000)).await.unwrap();
    let mut buf_after_backseek = vec![0u8; 16];
    stream.read_exact(&mut buf_after_backseek).await.unwrap();

    // Verify that seeking back returns the same bytes as the first seek to 1MB
    assert_eq!(
        buf, buf_after_backseek,
        "Bytes after seeking back should match original read"
    );
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
