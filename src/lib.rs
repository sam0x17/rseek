use std::io::{Error as IoError, ErrorKind, Result as IoResult};
use std::ops::Range;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{Buf, Bytes};
use reqwest::RequestBuilder;
use tokio::io::{AsyncRead, AsyncSeek, SeekFrom};

pub struct Seekable {
    request_template: RequestBuilder, // Store the original request to clone from
    position: u64,
    buffer: Bytes,
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
        })
    }

    async fn fetch_range(&mut self, range: Range<u64>) -> IoResult<Bytes> {
        let range_header = format!("bytes={}-{}", range.start, range.end - 1);

        let request = self
            .request_template
            .try_clone()
            .ok_or_else(|| IoError::new(ErrorKind::Other, "Failed to clone request template"))?
            .header("Range", range_header);

        let response = request
            .send()
            .await
            .map_err(|e| IoError::new(ErrorKind::Other, e.to_string()))?;

        Ok(response
            .bytes()
            .await
            .map_err(|e| IoError::new(ErrorKind::Other, e.to_string()))?)
    }
}

impl AsyncRead for Seekable {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<IoResult<()>> {
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
            SeekFrom::Current(offset) => (self.position as i64 + offset) as u64,
        };

        Ok(())
    }

    fn poll_complete(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<IoResult<u64>> {
        let new_range = self.position..self.position + 8192;
        let future = self.fetch_range(new_range);

        match futures::executor::block_on(future) {
            Ok(bytes) => {
                self.buffer = bytes;
                Poll::Ready(Ok(self.position))
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}
