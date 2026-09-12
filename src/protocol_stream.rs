//! Bounded remote-runtime framing over application-authenticated byte streams.
//!
//! The embedding daemon owns the host across connections. Authenticate and authorize
//! the peer before calling [`serve_authenticated_stream`]. Use a separate host per
//! security principal: runtime IDs are not authorization credentials. This module
//! deliberately does not bind a public listener or transfer host credentials.

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::Mutex;

use crate::protocol::{
    decode_client_frame, decode_host_frame, encode_client_frame, encode_host_frame, ClientFrame,
    HostFrame, ProtocolCodecError, MAX_PROTOCOL_FRAME_BYTES,
};
use crate::protocol_client::{RemoteRuntimeConnection, RemoteTransportError};
use crate::protocol_host::{
    HostFrameSink, HostFrameSinkError, RemoteRuntimeHost, RemoteRuntimeHostError,
};

/// A framed connection failed. Diagnostics never contain frame payloads.
#[derive(Debug, thiserror::Error)]
pub enum ProtocolStreamError {
    /// Underlying stream read or write failed.
    #[error("runtime protocol stream I/O failed: {0}")]
    Io(#[from] std::io::Error),
    /// Peer announced an invalid frame size, rejected before allocation.
    #[error("runtime protocol frame size {size} is outside 1..={MAX_PROTOCOL_FRAME_BYTES}")]
    FrameSize {
        /// Announced or encoded payload size.
        size: usize,
    },
    /// A frame did not satisfy the SDK protocol.
    #[error("runtime protocol frame validation failed")]
    Codec(#[source] ProtocolCodecError),
    /// Host dispatch failed.
    #[error("runtime protocol host dispatch failed: {0}")]
    Host(#[from] RemoteRuntimeHostError),
    /// A peer did not finish its next frame before the carrier deadline.
    #[error("runtime protocol frame read timed out after {timeout:?}")]
    ReadTimeout {
        /// Maximum time allowed for the complete length-prefixed frame.
        timeout: Duration,
    },
}

/// Default deadline for receiving one complete client frame.
pub const DEFAULT_FRAME_READ_TIMEOUT: Duration = Duration::from_secs(60);

fn check_size(size: usize) -> Result<(), ProtocolStreamError> {
    if size == 0 || size > MAX_PROTOCOL_FRAME_BYTES {
        return Err(ProtocolStreamError::FrameSize { size });
    }
    Ok(())
}

async fn read_frame<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> Result<Option<Vec<u8>>, ProtocolStreamError> {
    let mut prefix = [0; 4];
    if reader.read(&mut prefix[..1]).await? == 0 {
        return Ok(None);
    }
    reader.read_exact(&mut prefix[1..]).await?;
    let size = u32::from_be_bytes(prefix) as usize;
    check_size(size)?;
    let mut bytes = vec![0; size];
    reader.read_exact(&mut bytes).await?;
    Ok(Some(bytes))
}

async fn write_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    bytes: &[u8],
) -> Result<(), ProtocolStreamError> {
    check_size(bytes.len())?;
    let size = u32::try_from(bytes.len())
        .map_err(|_| ProtocolStreamError::FrameSize { size: bytes.len() })?;
    writer.write_all(&size.to_be_bytes()).await?;
    writer.write_all(bytes).await?;
    writer.flush().await?;
    Ok(())
}

/// SDK client connection over an already authenticated stream.
///
/// Return a fresh instance from your [`crate::protocol_client::RemoteRuntimeConnector`]
/// on reconnect. Reads and writes are independently serialized and full duplex.
pub struct AuthenticatedStreamConnection<R, W> {
    reader: Mutex<R>,
    writer: Mutex<W>,
}

impl<R, W> AuthenticatedStreamConnection<R, W> {
    /// Wraps the read/write halves after application-owned authentication.
    pub fn new(reader: R, writer: W) -> Self {
        Self {
            reader: Mutex::new(reader),
            writer: Mutex::new(writer),
        }
    }
}

fn transport_error(error: ProtocolStreamError) -> RemoteTransportError {
    match error {
        ProtocolStreamError::Io(_) => RemoteTransportError::Disconnected {
            message: "runtime byte stream closed or failed".into(),
        },
        _ => RemoteTransportError::Rejected {
            message: "runtime byte stream rejected an invalid protocol frame".into(),
        },
    }
}

#[async_trait]
impl<R, W> RemoteRuntimeConnection for AuthenticatedStreamConnection<R, W>
where
    R: AsyncRead + Unpin + Send,
    W: AsyncWrite + Unpin + Send,
{
    async fn send(&self, frame: ClientFrame) -> Result<(), RemoteTransportError> {
        let bytes = encode_client_frame(&frame)
            .map_err(|e| transport_error(ProtocolStreamError::Codec(e)))?;
        write_frame(&mut *self.writer.lock().await, &bytes)
            .await
            .map_err(transport_error)
    }

    async fn receive(&self) -> Result<HostFrame, RemoteTransportError> {
        let bytes = read_frame(&mut *self.reader.lock().await)
            .await
            .map_err(transport_error)?
            .ok_or_else(|| RemoteTransportError::Disconnected {
                message: "runtime byte stream closed".into(),
            })?;
        decode_host_frame(&bytes).map_err(|e| transport_error(ProtocolStreamError::Codec(e)))
    }
}

struct StreamSink<W>(Mutex<W>);

#[async_trait]
impl<W: AsyncWrite + Unpin + Send> HostFrameSink for StreamSink<W> {
    async fn send(&self, frame: HostFrame) -> Result<(), HostFrameSinkError> {
        let bytes = encode_host_frame(&frame).map_err(|_| HostFrameSinkError {
            message: "invalid outgoing runtime frame".into(),
        })?;
        write_frame(&mut *self.0.lock().await, &bytes)
            .await
            .map_err(|_| HostFrameSinkError {
                message: "runtime byte stream closed or failed".into(),
            })
    }
}

/// Serves one authenticated connection against a daemon-owned retained host.
///
/// EOF does not dispose runtimes, cancel turns, or stop managed processes. The
/// caller must retain `host` for subsequent reconnect/replay. Do not abort this
/// future merely because the peer disconnects: accepted dispatches must finish.
/// This compatibility entry point applies [`DEFAULT_FRAME_READ_TIMEOUT`] to each
/// complete incoming frame. Use [`serve_authenticated_stream_with_read_timeout`]
/// when the embedding carrier needs a different bounded deadline.
pub async fn serve_authenticated_stream<R, W>(
    host: &RemoteRuntimeHost,
    reader: R,
    writer: W,
) -> Result<(), ProtocolStreamError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin + Send + 'static,
{
    serve_authenticated_stream_with_read_timeout(host, reader, writer, DEFAULT_FRAME_READ_TIMEOUT)
        .await
}

/// Serves one authenticated stream with a whole-frame read deadline.
///
/// The timeout covers both the four-byte length prefix and the complete body,
/// preventing idle or slowly-dripped frames from retaining a connection slot.
/// Host dispatch begins only after a complete frame and is not subject to this
/// carrier deadline.
pub async fn serve_authenticated_stream_with_read_timeout<R, W>(
    host: &RemoteRuntimeHost,
    mut reader: R,
    writer: W,
    read_timeout: Duration,
) -> Result<(), ProtocolStreamError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let sink: Arc<dyn HostFrameSink> = Arc::new(StreamSink(Mutex::new(writer)));
    loop {
        let bytes = tokio::time::timeout(read_timeout, read_frame(&mut reader))
            .await
            .map_err(|_| ProtocolStreamError::ReadTimeout {
                timeout: read_timeout,
            })??;
        let Some(bytes) = bytes else {
            return Ok(());
        };
        let frame = decode_client_frame(&bytes).map_err(ProtocolStreamError::Codec)?;
        host.dispatch(frame, Arc::clone(&sink)).await?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestConnector(RemoteRuntimeHost);

    #[async_trait]
    impl crate::protocol_client::RemoteRuntimeConnector for TestConnector {
        async fn connect(&self) -> Result<Arc<dyn RemoteRuntimeConnection>, RemoteTransportError> {
            let (client, server) = tokio::io::duplex(4096);
            let host = self.0.clone();
            tokio::spawn(async move {
                let (reader, writer) = tokio::io::split(server);
                serve_authenticated_stream(&host, reader, writer).await
            });
            let (reader, writer) = tokio::io::split(client);
            Ok(Arc::new(AuthenticatedStreamConnection::new(reader, writer)))
        }
    }

    #[tokio::test]
    async fn new_client_attaches_to_daemon_owned_runtime() {
        use crate::journal::{EventJournalLimits, InMemoryEventJournal};
        use crate::lifecycle::RuntimeId;
        use crate::protocol_client::RemoteRuntimeClient;
        use crate::retained::{InProcessRuntimeClient, RuntimeClient, RuntimeSpec};

        let runtime = crate::AgentRuntime::builder().build().unwrap();
        let journal = InMemoryEventJournal::new(EventJournalLimits::default()).unwrap();
        let host = RemoteRuntimeHost::new(
            Arc::new(InProcessRuntimeClient::new(runtime)),
            Arc::new(journal),
        );
        let connector = Arc::new(TestConnector(host));
        let id = RuntimeId::new("stream-retained-test").unwrap();
        let first = RemoteRuntimeClient::new(connector.clone());
        let handle = first
            .acquire(RuntimeSpec::new(id.clone(), crate::Provider::Codex, "/tmp"))
            .await
            .unwrap();
        drop(handle);
        drop(first);
        let second = RemoteRuntimeClient::new(connector);
        second.attach(&id).await.unwrap();
        second.dispose(&id).await.unwrap();
        assert!(second.attach(&id).await.is_err());
    }

    #[tokio::test]
    async fn framing_round_trip_and_clean_eof() {
        let mut bytes = Vec::new();
        write_frame(&mut bytes, b"first").await.unwrap();
        write_frame(&mut bytes, b"second").await.unwrap();
        let mut input = bytes.as_slice();
        assert_eq!(
            read_frame(&mut input).await.unwrap(),
            Some(b"first".to_vec())
        );
        assert_eq!(
            read_frame(&mut input).await.unwrap(),
            Some(b"second".to_vec())
        );
        assert!(read_frame(&mut input).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn rejects_oversized_and_empty_frames_before_body_read() {
        for size in [0_u32, u32::MAX] {
            let prefix = size.to_be_bytes();
            assert!(matches!(
                read_frame(&mut prefix.as_slice()).await,
                Err(ProtocolStreamError::FrameSize { .. })
            ));
        }
    }

    #[tokio::test]
    async fn partial_prefix_and_body_are_errors_not_clean_disconnects() {
        for bytes in [vec![0, 0], vec![0, 0, 0, 3, b'a']] {
            assert!(matches!(
                read_frame(&mut bytes.as_slice()).await,
                Err(ProtocolStreamError::Io(_))
            ));
        }
    }

    #[tokio::test]
    async fn whole_frame_deadline_rejects_a_slow_partial_frame() {
        use crate::journal::InMemoryEventJournal;
        use crate::retained::InProcessRuntimeClient;

        let runtime = crate::AgentRuntime::builder().build().unwrap();
        let host = RemoteRuntimeHost::new(
            Arc::new(InProcessRuntimeClient::new(runtime)),
            Arc::new(InMemoryEventJournal::default()),
        );
        let (mut client, server) = tokio::io::duplex(64);
        client.write_all(&[0, 0]).await.unwrap();
        let (reader, writer) = tokio::io::split(server);
        let result = serve_authenticated_stream_with_read_timeout(
            &host,
            reader,
            writer,
            Duration::from_millis(20),
        )
        .await;
        assert!(matches!(
            result,
            Err(ProtocolStreamError::ReadTimeout { timeout })
                if timeout == Duration::from_millis(20)
        ));
    }
}
