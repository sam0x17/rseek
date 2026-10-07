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
    StreamReader<Pin<Box<dyn futures::Stream<Item = Result<Bytes, IoError>> + Send + Sync>>, Bytes>;

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
    init_fetch: Option<Pin<Box<dyn futures::Future<Output = IoResult<Response>> + Send + Sync>>>,
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
            .and_then(Response::error_for_status)
            .map_err(IoError::other)?;
        if resp.status() != reqwest::StatusCode::PARTIAL_CONTENT {
            // Range not supported
            return Err(IoError::new(
                ErrorKind::Unsupported,
                "server does not support range requests",
            ));
        }
        // Parse Content-Range header: bytes 0-0/size
        if let Some(cr) = resp.headers().get("content-range")
            && let Ok(s) = cr.to_str()
            && let Some(total) = s.split('/').nth(1)
            && let Ok(n) = total.parse::<u64>()
        {
            return Ok(n);
        }
        Err(IoError::other("failed to determine file size"))
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
                .and_then(Response::error_for_status)
                .map_err(IoError::other)
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
        if let Some(sz) = this.file_size
            && this.position >= sz
        {
            return Poll::Ready(Err(IoError::new(ErrorKind::UnexpectedEof, "EOF reached")));
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
                        .map_err(|e| IoError::other(e.to_string()));
                    this.reader = Some(StreamReader::new(Box::pin(stream)));
                    this.init_fetch = None;
                    // Recurse into reader
                    let pinned = unsafe { Pin::new_unchecked(this) };
                    return AsyncRead::poll_read(pinned, cx, buf);
                }
                Poll::Ready(Err(e)) => {
                    this.init_fetch = None;
                    return Poll::Ready(Err(e));
                }
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
