Provides a seekable and asynchronous read interface for `reqwest` HTTP streams that allows
you to seek forward or backward in an HTTP stream without having to download all the
intermediate data. This is useful for handling large files over HTTP where random access is
required.

## Example
```rust
use reqwest::Client;
use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};

#[tokio::main]
async fn main() {
    use rseek::Seekable;
    let client = Client::new();
    let mut stream = Seekable::new(move || client.get("https://example.com/largefile.bin")).await;

    let mut buf = vec![0u8; 16];
    stream.read_exact(&mut buf).await.unwrap();
    println!("First 16 bytes: {:?}", buf);

    stream.seek(SeekFrom::Start(1_000_000)).await.unwrap();
    stream.read_exact(&mut buf).await.unwrap();
    println!("Bytes after seeking to 1MB: {:?}", buf);
 }
 ```

## Notes
- This implementation assumes the server supports HTTP range requests. Servers that do not
  support range requests are still usable, however certain seeking features will be
  unavailable.
- If the file size cannot be determined, the implementation will attempt to fetch data without
  bounds, relying on the server to handle the request appropriately.

## Errors
- Returns `UnexpectedEof` if attempting to read past the end of the file.
- Returns `InvalidInput` if seeking to a negative position.
- Returns `Unsupported` if seeking from the end when the file size is unknown.
