//! What TDS travels over: TCP to a server, or, on Windows, the named pipe of a LocalDB instance
//! (SDB-312). Both are plain byte streams to tiberius.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

pub enum Transport {
    Tcp(TcpStream),
    #[cfg(windows)]
    Pipe(tokio::net::windows::named_pipe::NamedPipeClient),
}

#[cfg(windows)]
impl Transport {
    /// Opens the pipe `path` (`\\.\pipe\…`), waiting while it is busy with another client's
    /// connection, up to the caller's timeout.
    pub async fn pipe(path: &str) -> io::Result<Self> {
        use tokio::net::windows::named_pipe::ClientOptions;
        // ERROR_PIPE_BUSY: every instance of the pipe is taken for a moment.
        const PIPE_BUSY: i32 = 231;
        loop {
            match ClientOptions::new().open(path) {
                Ok(client) => return Ok(Transport::Pipe(client)),
                Err(e) if e.raw_os_error() == Some(PIPE_BUSY) => {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
                Err(e) => return Err(e),
            }
        }
    }
}

/// TEMPORARY DIAGNOSTIC (SDB-462): what crosses the pipe.
#[cfg(windows)]
fn trace(what: &str, bytes: &[u8], result: &str) {
    let head: String = bytes
        .iter()
        .take(48)
        .map(|b| format!("{b:02x}"))
        .collect::<Vec<_>>()
        .join(" ");
    eprintln!("PIPE {what} {} bytes {result}: {head}", bytes.len());
}

impl AsyncRead for Transport {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Transport::Tcp(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(windows)]
            Transport::Pipe(s) => {
                let before = buf.filled().len();
                let polled = Pin::new(s).poll_read(cx, buf);
                match &polled {
                    Poll::Ready(Ok(())) => trace("read", &buf.filled()[before..], "ok"),
                    Poll::Ready(Err(e)) => trace("read", &[], &format!("error {e}")),
                    Poll::Pending => {}
                }
                polled
            }
        }
    }
}

impl AsyncWrite for Transport {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Transport::Tcp(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(windows)]
            Transport::Pipe(s) => {
                let polled = Pin::new(s).poll_write(cx, buf);
                match &polled {
                    Poll::Ready(Ok(n)) => trace("write", &buf[..*n], "ok"),
                    Poll::Ready(Err(e)) => trace("write", buf, &format!("error {e}")),
                    Poll::Pending => {}
                }
                polled
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Transport::Tcp(s) => Pin::new(s).poll_flush(cx),
            #[cfg(windows)]
            Transport::Pipe(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Transport::Tcp(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(windows)]
            Transport::Pipe(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}
