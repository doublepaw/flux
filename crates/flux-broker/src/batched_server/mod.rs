// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Batched broker server with buffering and deduplication.
//!
//! Extends the basic server with:
//! - Request buffering and merging
//! - Periodic flush to S3
//! - Deduplication via LRU cache
//! - Backpressure handling

mod append;
mod authz;
mod encoding;
mod fetch;
mod flush;
mod raw_read;
mod read;
mod read_ahead;
mod reader_group;

use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, LazyLock, Mutex as StdMutex};
use std::time::{Duration, Instant};

use tokio_util::sync::CancellationToken;

use bumpalo::Bump;
use futures::{SinkExt, StreamExt};
use sqlx::PgPool;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, RwLock, mpsc};
use tokio_tungstenite::{accept_async, tungstenite::Message};
use tracing::{error, info, warn};

use flux_common::ids::{AppendSeq, WriterId};
use flux_common::types::{BatchAck, RecordBatch};
use flux_wire::{
    ClientMessage, ERR_AUTHZ_DENIED, ERR_DECODE_ERROR, ERR_INTERNAL_ERROR,
    ERR_INVALID_MESSAGE_TYPE, ServerMessage, auth as wire_auth, decode_client_message,
    encode_server_message,
};

use crate::BrokerError;
use crate::auth::{AclChecker, ApiKeyValidator, AuthError, Principal};
use crate::buffer::BufferConfig;
use crate::coordinator::{Coordinator, CoordinatorConfig};
use crate::dedup::DedupCache;
use crate::metrics::{
    APPEND_LATENCY_SECONDS, APPEND_REQUESTS_TOTAL, BUFFER_SIZE_BYTES, ConnectionGuard,
    FLUSH_QUEUE_DEPTH, WS_DECODE_CLIENT_SECONDS, WS_HANDLE_MESSAGE_SECONDS,
    WS_OUTBOUND_CHANNEL_WAIT_SECONDS, WS_OUTBOUND_QUEUE_WAIT_SECONDS, WS_WRITE_SECONDS,
};
use crate::object_store::ObjectStore;

use append::{EnqueueResult, WriterInFlightState, await_append_ack, enqueue_append};
use authz::{can_append, can_consume_topic, can_group_consume, can_read};
use encoding::{ErrorResponseKind, client_message_kind, encode_error_response, error_kind_label};
use flush::flush_loop;
use read::handle_read_request;
use reader_group::{
    handle_commit, handle_heartbeat, handle_join_group, handle_leave_group, handle_poll,
};

const DEFAULT_WRITER_STATE_BUMP_CAPACITY_BYTES: usize = 512 * 1024 * 1024;
static WRITER_STATE_ROWS_BUMP: LazyLock<StdMutex<Bump>> = LazyLock::new(|| {
    let capacity_bytes = std::env::var("FLUX_WRITER_STATE_BUMP_BYTES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|bytes| *bytes > 0)
        .unwrap_or(DEFAULT_WRITER_STATE_BUMP_CAPACITY_BYTES);
    StdMutex::new(Bump::with_capacity(capacity_bytes))
});

/// Batched broker configuration.
#[derive(Debug, Clone)]
pub struct BrokerConfig {
    /// Address to bind to.
    pub bind_addr: SocketAddr,
    /// S3 bucket for storing FL files.
    pub bucket: String,
    /// S3 key prefix for FL files.
    pub key_prefix: String,
    /// Buffer configuration.
    pub buffer: BufferConfig,
    /// Flush interval for the buffer.
    pub flush_interval: Duration,
    /// Whether to require authentication.
    pub require_auth: bool,
    /// Auth timeout for connection handshake.
    pub auth_timeout: Duration,
    /// Byte cap for the direct-read read-ahead cache (0 = disabled).
    pub readahead_max_bytes: usize,
    /// Concurrent flushes in flight (object-store puts overlap; commits stay
    /// in flush order). 1 = classic serial flushing.
    pub flush_pipeline_depth: usize,
    /// Iceberg ingestion config (None = disabled).
    #[cfg(feature = "iceberg")]
    pub iceberg: Option<flux_iceberg::IcebergConfig>,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:9000".parse().unwrap(),
            bucket: "flux".to_string(),
            key_prefix: "data".to_string(),
            buffer: BufferConfig::default(),
            flush_interval: Duration::from_millis(100),
            require_auth: false,
            auth_timeout: Duration::from_secs(10),
            readahead_max_bytes: 0,
            flush_pipeline_depth: 4,
            #[cfg(feature = "iceberg")]
            iceberg: None,
        }
    }
}

/// Message sent to the flush task.
#[allow(dead_code)] // ForceFlush and Shutdown are for graceful shutdown
enum FlushCommand {
    /// Insert a append request into the buffer.
    Insert {
        writer_id: WriterId,
        append_seq: AppendSeq,
        batches: Vec<RecordBatch>,
        enqueued_at: Instant,
        response_tx: tokio::sync::oneshot::Sender<Vec<BatchAck>>,
    },
    /// Force an immediate flush.
    ForceFlush,
    /// Shutdown the flush task.
    Shutdown,
}

/// Shared state for the batched broker.
pub struct BrokerState<S: ObjectStore> {
    pub pool: PgPool,
    pub store: Arc<S>,
    pub config: BrokerConfig,
    pub dedup_cache: DedupCache,
    pub coordinator: Coordinator,
    pub api_key_validator: ApiKeyValidator,
    pub acl_checker: AclChecker,
    /// Channel to send commands to the flush task.
    flush_tx: mpsc::Sender<FlushCommand>,
    /// Flag indicating if backpressure is active.
    backpressure: Arc<RwLock<bool>>,
    /// In-flight append requests grouped per writer.
    in_flight_append: Arc<Mutex<HashMap<WriterId, WriterInFlightState>>>,
    /// Number of queued append commands waiting in flush loop.
    pending_flush_commands: Arc<AtomicUsize>,
    /// Token cancelled on hard crash to stop flush loop and connection handlers.
    cancel_token: CancellationToken,
    /// Random per-broker token embedded in flush object keys. Brokers
    /// sharing a bucket otherwise collide: timestamp+counter alone repeats
    /// across processes started together, and the last PUT silently
    /// overwrites the other broker's committed segments.
    pub(crate) flush_key_instance: u64,
    /// Read-ahead cache for direct reads (prefetches the next window).
    pub(crate) readahead: read_ahead::ReadAheadCache<(
        Vec<flux_wire::reader::TopicResult>,
        flux_common::ids::Offset,
    )>,
    /// Read-ahead cache for raw (zero-copy) reads.
    pub(crate) raw_readahead:
        read_ahead::ReadAheadCache<(Vec<flux_wire::reader::RawSegment>, flux_common::ids::Offset)>,
    /// Iceberg ingestion buffer (None = disabled).
    #[cfg(feature = "iceberg")]
    pub iceberg_buffer: Option<Arc<flux_iceberg::IcebergBuffer>>,
}

impl<S: ObjectStore + Send + Sync + 'static> BrokerState<S> {
    /// Create new batched broker state and spawn flush task.
    pub async fn new(pool: PgPool, store: S, config: BrokerConfig) -> Arc<Self> {
        Self::with_coordinator_config(pool, store, config, CoordinatorConfig::default()).await
    }

    /// Create new batched broker state with custom coordinator config.
    pub async fn with_coordinator_config(
        pool: PgPool,
        store: S,
        config: BrokerConfig,
        coordinator_config: CoordinatorConfig,
    ) -> Arc<Self> {
        let (flush_tx, flush_rx) = mpsc::channel(1000);
        let dedup_cache = DedupCache::new(pool.clone());
        let coordinator = Coordinator::new(pool.clone(), coordinator_config);
        let api_key_validator = ApiKeyValidator::new(pool.clone());
        let acl_checker = AclChecker::new(pool.clone());
        let store = Arc::new(store);
        let backpressure = Arc::new(RwLock::new(false));
        let in_flight_append = Arc::new(Mutex::new(HashMap::new()));
        let pending_flush_commands = Arc::new(AtomicUsize::new(0));
        let cancel_token = CancellationToken::new();

        // Initialize Iceberg buffer if configured
        #[cfg(feature = "iceberg")]
        let iceberg_buffer = {
            if let Some(ref iceberg_config) = config.iceberg {
                match flux_iceberg::iceberg_writer::TableWriter::new(
                    iceberg_config.clone(),
                    pool.clone(),
                )
                .await
                {
                    Ok(writer) => {
                        info!("Iceberg ingestion enabled");
                        Some(flux_iceberg::IcebergBuffer::new(
                            iceberg_config.clone(),
                            writer,
                        ))
                    }
                    Err(e) => {
                        error!("Failed to initialize Iceberg writer: {}", e);
                        None
                    }
                }
            } else {
                None
            }
        };

        let state = Arc::new(Self {
            pool: pool.clone(),
            store: store.clone(),
            config: config.clone(),
            dedup_cache,
            coordinator,
            api_key_validator,
            acl_checker,
            flush_tx,
            backpressure: backpressure.clone(),
            in_flight_append,
            pending_flush_commands,
            cancel_token,
            flush_key_instance: uuid::Uuid::new_v4().as_u128() as u64,
            readahead: read_ahead::ReadAheadCache::new(config.readahead_max_bytes),
            raw_readahead: read_ahead::ReadAheadCache::new(config.readahead_max_bytes),
            #[cfg(feature = "iceberg")]
            iceberg_buffer,
        });
        FLUSH_QUEUE_DEPTH.set(0.0);
        BUFFER_SIZE_BYTES.set(0.0);

        // Spawn flush task
        let flush_state = state.clone();
        tokio::spawn(async move {
            flush_loop(flush_rx, flush_state).await;
        });

        state
    }

    /// Get a clone of the cancellation token for crash simulation.
    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel_token.clone()
    }

    /// Check if backpressure is active.
    pub async fn is_backpressure_active(&self) -> bool {
        *self.backpressure.read().await
    }
}

/// Default drain timeout for graceful shutdown.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(25);

/// Run the batched broker server.
pub async fn run<S: ObjectStore + Send + Sync + 'static>(
    state: Arc<BrokerState<S>>,
) -> Result<(), BrokerError> {
    run_with_shutdown(state, std::future::pending()).await
}

/// Run the batched broker server with graceful shutdown support.
pub async fn run_with_shutdown<S, F>(
    state: Arc<BrokerState<S>>,
    shutdown: F,
) -> Result<(), BrokerError>
where
    S: ObjectStore + Send + Sync + 'static,
    F: Future<Output = ()> + Send,
{
    use crate::shutdown::ConnectionTracker;

    let listener = TcpListener::bind(&state.config.bind_addr).await?;
    info!("Batched broker listening on {}", state.config.bind_addr);

    let tracker = Arc::new(ConnectionTracker::new());
    let mut shutdown = Pin::from(Box::new(shutdown));

    loop {
        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((stream, addr)) => {
                        let state = state.clone();
                        let conn_tracker = tracker.clone();

                        tokio::spawn(async move {
                            // Track connection for graceful shutdown
                            let _tracked = crate::shutdown::TrackedConnection::new(conn_tracker);
                            if let Err(e) = handle_connection(stream, addr, state).await {
                                error!("Connection error from {}: {}", addr, e);
                            }
                        });
                    }
                    Err(e) => {
                        error!("Failed to accept connection: {}", e);
                    }
                }
            }
            _ = &mut shutdown => {
                info!("Shutdown signal received, draining connections...");
                break;
            }
        }
    }

    // Stop accepting new connections and drain existing ones
    let drained = tracker.wait_for_drain(DRAIN_TIMEOUT).await;
    if !drained {
        warn!(
            "Drain timeout reached, {} connections still active",
            tracker.count()
        );
    }

    // Flush remaining buffer before exit
    info!("Flushing remaining buffer...");
    if state.flush_tx.send(FlushCommand::Shutdown).await.is_err() {
        error!("Failed to send shutdown command to flush task");
    }

    // Give the flush task time to complete
    tokio::time::sleep(Duration::from_secs(2)).await;

    info!("Graceful shutdown complete");
    Ok(())
}

/// Connection context with authenticated principal.
#[derive(Clone)]
struct ConnectionContext {
    principal: Option<Principal>,
}

enum OutboundMessage {
    Binary { data: Vec<u8>, enqueued_at: Instant },
    Pong { data: Vec<u8>, enqueued_at: Instant },
}

/// Send a binary response through the outbound channel. Returns false if channel is closed.
async fn send_outbound(out_tx: &mpsc::Sender<OutboundMessage>, data: Vec<u8>) -> bool {
    let reserve_started = Instant::now();
    match out_tx.reserve().await {
        Ok(permit) => {
            WS_OUTBOUND_CHANNEL_WAIT_SECONDS.observe(reserve_started.elapsed().as_secs_f64());
            permit.send(OutboundMessage::Binary {
                data,
                enqueued_at: Instant::now(),
            });
            true
        }
        Err(_) => false,
    }
}

/// Reader group membership tracked per connection for cleanup on disconnect.
type ReaderMemberships = Arc<StdMutex<Vec<(String, flux_common::ids::TopicId, String)>>>;

/// Handle a single WebSocket connection.
async fn handle_connection<S: ObjectStore + Send + Sync + 'static>(
    stream: TcpStream,
    addr: SocketAddr,
    state: Arc<BrokerState<S>>,
) -> Result<(), BrokerError> {
    let _conn_guard = ConnectionGuard::new();
    let ws_stream = accept_async(stream).await?;
    info!("New WebSocket connection from {}", addr);

    let (mut write, mut read) = ws_stream.split();

    // Auth handshake if required
    let ctx = if state.config.require_auth {
        match perform_auth_handshake(&mut read, &mut write, &state).await {
            Ok(principal) => {
                info!("Authenticated connection from {}: {}", addr, principal.id);
                ConnectionContext {
                    principal: Some(principal),
                }
            }
            Err(e) => {
                warn!("Auth failed for {}: {}", addr, e);
                return Ok(());
            }
        }
    } else {
        ConnectionContext { principal: None }
    };

    let reader_memberships: ReaderMemberships = Arc::new(StdMutex::new(Vec::new()));

    let (out_tx, mut out_rx) = mpsc::channel::<OutboundMessage>(1024);
    let writer_task = tokio::spawn(async move {
        while let Some(outbound) = out_rx.recv().await {
            let send_result = match outbound {
                OutboundMessage::Binary { data, enqueued_at } => {
                    WS_OUTBOUND_QUEUE_WAIT_SECONDS.observe(enqueued_at.elapsed().as_secs_f64());
                    let write_started = Instant::now();
                    let result = write.send(Message::Binary(data)).await;
                    WS_WRITE_SECONDS
                        .with_label_values(&["binary"])
                        .observe(write_started.elapsed().as_secs_f64());
                    result
                }
                OutboundMessage::Pong { data, enqueued_at } => {
                    WS_OUTBOUND_QUEUE_WAIT_SECONDS.observe(enqueued_at.elapsed().as_secs_f64());
                    let write_started = Instant::now();
                    let result = write.send(Message::Pong(data)).await;
                    WS_WRITE_SECONDS
                        .with_label_values(&["pong"])
                        .observe(write_started.elapsed().as_secs_f64());
                    result
                }
            };
            if send_result.is_err() {
                break;
            }
        }
    });

    let cancel = state.cancel_token.clone();
    loop {
        let msg = tokio::select! {
            _ = cancel.cancelled() => break,
            result = read.next() => match result {
                Some(Ok(m)) => m,
                Some(Err(e)) => {
                    warn!("WebSocket error from {}: {}", addr, e);
                    break;
                }
                None => break,
            },
        };

        match msg {
            Message::Binary(data) => {
                let handle_started = Instant::now();
                let decode_started = Instant::now();
                let decoded = decode_client_message(&data);
                WS_DECODE_CLIENT_SECONDS.observe(decode_started.elapsed().as_secs_f64());

                // Extract append if the decode succeeds and consumes all bytes.
                // Appends are processed synchronously (enqueue_append) to
                // preserve per-writer record ordering; all other message types
                // are dispatched concurrently.
                let append_req = match decoded {
                    Ok((ClientMessage::Append(req), used)) if used == data.len() => Some(req),
                    _ => None,
                };

                if let Some(req) = append_req {
                    APPEND_REQUESTS_TOTAL.inc();

                    if !can_append(&state, ctx.principal.as_ref(), &req).await {
                        let response = encode_error_response(
                            ErrorResponseKind::Append,
                            ERR_AUTHZ_DENIED,
                            "append not authorized",
                        );
                        APPEND_LATENCY_SECONDS.observe(handle_started.elapsed().as_secs_f64());
                        if !send_outbound(&out_tx, response).await {
                            break;
                        }
                        continue;
                    }

                    let enqueued = enqueue_append(req, &state).await;
                    match enqueued {
                        EnqueueResult::Resolved(response) => {
                            APPEND_LATENCY_SECONDS.observe(handle_started.elapsed().as_secs_f64());
                            if !send_outbound(&out_tx, response).await {
                                break;
                            }
                        }
                        pending => {
                            let state = state.clone();
                            let out_tx = out_tx.clone();
                            tokio::spawn(async move {
                                let response = await_append_ack(pending, &state).await;
                                APPEND_LATENCY_SECONDS
                                    .observe(handle_started.elapsed().as_secs_f64());
                                send_outbound(&out_tx, response).await;
                            });
                        }
                    }
                } else {
                    // Track reader group membership for cleanup on disconnect.
                    // Optimistic: tracked before handler confirms success.
                    // Calling leave_group for a non-member is a safe no-op.
                    if let Ok((ref msg, used)) = decode_client_message(&data)
                        && used == data.len()
                    {
                        match msg {
                            ClientMessage::JoinGroup(req) => {
                                let tid = req
                                    .topic_ids
                                    .first()
                                    .copied()
                                    .unwrap_or(flux_common::ids::TopicId(0));
                                reader_memberships.lock().unwrap().push((
                                    req.group_id.clone(),
                                    tid,
                                    req.reader_id.clone(),
                                ));
                            }
                            ClientMessage::LeaveGroup(req) => {
                                let g = &req.group_id;
                                let t = req.topic_id;
                                let r = &req.reader_id;
                                reader_memberships
                                    .lock()
                                    .unwrap()
                                    .retain(|(g2, t2, r2)| !(g2 == g && *t2 == t && r2 == r));
                            }
                            _ => {}
                        }
                    }

                    // Non-append (or decode error): spawn concurrently.
                    // Re-decodes the message internally; the cost is
                    // negligible compared to the I/O work these handlers do.
                    let state = state.clone();
                    let ctx = ctx.clone();
                    let out_tx = out_tx.clone();
                    tokio::spawn(async move {
                        let response = handle_message(&data, &state, &ctx).await;
                        send_outbound(&out_tx, response).await;
                    });
                }
            }
            Message::Ping(data) => {
                let reserve_started = Instant::now();
                match out_tx.reserve().await {
                    Ok(permit) => {
                        WS_OUTBOUND_CHANNEL_WAIT_SECONDS
                            .observe(reserve_started.elapsed().as_secs_f64());
                        permit.send(OutboundMessage::Pong {
                            data,
                            enqueued_at: Instant::now(),
                        });
                    }
                    Err(_) => break,
                }
            }
            Message::Close(_) => {
                info!("Connection closed by {}", addr);
                break;
            }
            _ => {}
        }
    }

    // Proactive leave_group on disconnect: release reader group ranges
    // so other readers can pick up the work immediately.
    let memberships = reader_memberships.lock().unwrap().clone();
    for (group_id, topic_id, reader_id) in memberships {
        if let Err(e) = state
            .coordinator
            .leave_group(&group_id, topic_id, &reader_id)
            .await
        {
            warn!(
                "Failed to leave group on disconnect: group={} reader={}: {}",
                group_id, reader_id, e
            );
        }
    }

    drop(out_tx);
    let _ = writer_task.await;

    Ok(())
}

/// Perform auth handshake with client.
#[tracing::instrument(level = "debug", skip(read, write, state))]
async fn perform_auth_handshake<S: ObjectStore + Send + Sync + 'static, R, W>(
    read: &mut R,
    write: &mut W,
    state: &BrokerState<S>,
) -> Result<Principal, AuthError>
where
    R: futures::Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
    W: futures::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    // Wait for Auth message with timeout
    let auth_msg = tokio::time::timeout(state.config.auth_timeout, read.next())
        .await
        .map_err(|_| AuthError::Timeout)?
        .ok_or(AuthError::MissingApiKey)?
        .map_err(|_| AuthError::MissingApiKey)?;

    let data = match auth_msg {
        Message::Binary(d) => d,
        _ => return Err(AuthError::InvalidKeyFormat),
    };

    let (msg, used) = decode_client_message(&data).map_err(|_| AuthError::InvalidKeyFormat)?;
    if used != data.len() {
        return Err(AuthError::InvalidKeyFormat);
    }
    let auth_req = match msg {
        ClientMessage::Auth(req) => req,
        _ => return Err(AuthError::InvalidKeyFormat),
    };

    // Validate API key
    let result = state.api_key_validator.validate(&auth_req.api_key).await;

    // Send response
    let resp = match &result {
        Ok(_) => wire_auth::AuthResponse {
            success: true,
            error_message: String::new(),
            error_code: 0,
        },
        Err(e) => wire_auth::AuthResponse {
            success: false,
            error_message: e.to_string(),
            error_code: e.error_code(),
        },
    };

    let mut buf = vec![0u8; 512];
    let len = encode_server_message(&ServerMessage::Auth(resp), &mut buf)
        .map_err(|_| AuthError::InvalidKeyFormat)?;
    buf.truncate(len);

    write
        .send(Message::Binary(buf))
        .await
        .map_err(|_| AuthError::Timeout)?;

    result
}

/// Handle a binary message (non-append path).
async fn handle_message<S: ObjectStore + Send + Sync + 'static>(
    data: &[u8],
    state: &Arc<BrokerState<S>>,
    ctx: &ConnectionContext,
) -> Vec<u8> {
    let handle_started = Instant::now();
    let observe_and_return = |kind: &'static str, response: Vec<u8>| -> Vec<u8> {
        WS_HANDLE_MESSAGE_SECONDS
            .with_label_values(&[kind])
            .observe(handle_started.elapsed().as_secs_f64());
        response
    };

    let decode_started = Instant::now();
    let (msg, used) = match decode_client_message(data) {
        Ok(decoded) => decoded,
        Err(_) => {
            WS_DECODE_CLIENT_SECONDS.observe(decode_started.elapsed().as_secs_f64());
            if let Ok((key, _)) = flux_wire::varint::decode_u64(data) {
                let field = key >> 3;
                let wire_type = key & 0x07;
                if wire_type != 2 {
                    return observe_and_return(
                        "decode_error",
                        encode_error_response(
                            ErrorResponseKind::Unknown,
                            ERR_INVALID_MESSAGE_TYPE,
                            "invalid proto-framed request",
                        ),
                    );
                }
                let (kind, decode_msg) = match field {
                    1 => (ErrorResponseKind::Append, "failed to decode append request"),
                    2 => (ErrorResponseKind::Read, "failed to decode read request"),
                    3 => (
                        ErrorResponseKind::JoinGroup,
                        "failed to decode join request",
                    ),
                    4 => (
                        ErrorResponseKind::Heartbeat,
                        "failed to decode heartbeat request",
                    ),
                    5 => (ErrorResponseKind::Poll, "failed to decode poll request"),
                    6 => (
                        ErrorResponseKind::LeaveGroup,
                        "failed to decode leave request",
                    ),
                    7 => (ErrorResponseKind::Commit, "failed to decode commit request"),
                    8 => (
                        ErrorResponseKind::Unknown,
                        "auth request is only valid during handshake",
                    ),
                    _ => (ErrorResponseKind::Unknown, "invalid proto-framed request"),
                };
                if matches!(
                    kind,
                    ErrorResponseKind::Append
                        | ErrorResponseKind::Read
                        | ErrorResponseKind::JoinGroup
                        | ErrorResponseKind::Heartbeat
                        | ErrorResponseKind::Poll
                        | ErrorResponseKind::LeaveGroup
                        | ErrorResponseKind::Commit
                ) {
                    return observe_and_return(
                        error_kind_label(kind),
                        encode_error_response(kind, ERR_DECODE_ERROR, decode_msg),
                    );
                }
                return observe_and_return(
                    error_kind_label(kind),
                    encode_error_response(kind, ERR_INVALID_MESSAGE_TYPE, decode_msg),
                );
            }
            return observe_and_return(
                "decode_error",
                encode_error_response(
                    ErrorResponseKind::Unknown,
                    ERR_INVALID_MESSAGE_TYPE,
                    "invalid proto-framed request",
                ),
            );
        }
    };
    WS_DECODE_CLIENT_SECONDS.observe(decode_started.elapsed().as_secs_f64());

    if used != data.len() {
        return observe_and_return(
            "decode_error",
            encode_error_response(
                ErrorResponseKind::Unknown,
                ERR_DECODE_ERROR,
                "trailing bytes in request",
            ),
        );
    }

    let kind = client_message_kind(&msg);
    let response = match msg {
        ClientMessage::Append(_) => {
            // Appends are routed through enqueue_append/await_append_ack
            // in the connection handler to preserve per-writer ordering.
            encode_error_response(
                ErrorResponseKind::Append,
                ERR_INTERNAL_ERROR,
                "unexpected append in message handler",
            )
        }
        ClientMessage::RawRead(req) => {
            // Reuse read authz: raw reads expose the same data.
            let as_read = flux_wire::reader::ReadRequest {
                topic_id: req.topic_id,
                offset: req.offset,
                max_bytes: req.max_bytes,
            };
            if !can_read(state, ctx.principal.as_ref(), &as_read).await {
                return observe_and_return(
                    "raw_read",
                    encode_error_response(
                        ErrorResponseKind::Read,
                        ERR_AUTHZ_DENIED,
                        "read not authorized",
                    ),
                );
            }
            let response = raw_read::handle_raw_read_request(req, state).await;
            observe_and_return("raw_read", response)
        }
        ClientMessage::Read(req) => {
            if !can_read(state, ctx.principal.as_ref(), &req).await {
                encode_error_response(
                    ErrorResponseKind::Read,
                    ERR_AUTHZ_DENIED,
                    "read not authorized",
                )
            } else {
                handle_read_request(req, state).await
            }
        }
        ClientMessage::JoinGroup(req) => {
            if !can_group_consume(state, ctx.principal.as_ref(), &req.group_id).await {
                encode_error_response(
                    ErrorResponseKind::JoinGroup,
                    ERR_AUTHZ_DENIED,
                    "group consume not authorized",
                )
            } else {
                let mut denied = false;
                for topic_id in &req.topic_ids {
                    if !can_consume_topic(state, ctx.principal.as_ref(), *topic_id).await {
                        denied = true;
                        break;
                    }
                }
                if denied {
                    encode_error_response(
                        ErrorResponseKind::JoinGroup,
                        ERR_AUTHZ_DENIED,
                        "topic consume not authorized",
                    )
                } else {
                    handle_join_group(req, state).await
                }
            }
        }
        ClientMessage::Heartbeat(req) => {
            if !can_group_consume(state, ctx.principal.as_ref(), &req.group_id).await
                || !can_consume_topic(state, ctx.principal.as_ref(), req.topic_id).await
            {
                encode_error_response(
                    ErrorResponseKind::Heartbeat,
                    ERR_AUTHZ_DENIED,
                    "heartbeat not authorized",
                )
            } else {
                handle_heartbeat(req, state).await
            }
        }
        ClientMessage::Poll(req) => {
            if !can_group_consume(state, ctx.principal.as_ref(), &req.group_id).await
                || !can_consume_topic(state, ctx.principal.as_ref(), req.topic_id).await
            {
                encode_error_response(
                    ErrorResponseKind::Poll,
                    ERR_AUTHZ_DENIED,
                    "poll not authorized",
                )
            } else {
                handle_poll(req, state).await
            }
        }
        ClientMessage::LeaveGroup(req) => {
            if !can_group_consume(state, ctx.principal.as_ref(), &req.group_id).await
                || !can_consume_topic(state, ctx.principal.as_ref(), req.topic_id).await
            {
                encode_error_response(
                    ErrorResponseKind::LeaveGroup,
                    ERR_AUTHZ_DENIED,
                    "leave not authorized",
                )
            } else {
                handle_leave_group(req, state).await
            }
        }
        ClientMessage::Commit(req) => {
            if !can_group_consume(state, ctx.principal.as_ref(), &req.group_id).await
                || !can_consume_topic(state, ctx.principal.as_ref(), req.topic_id).await
            {
                encode_error_response(
                    ErrorResponseKind::Commit,
                    ERR_AUTHZ_DENIED,
                    "commit not authorized",
                )
            } else {
                handle_commit(req, state).await
            }
        }
        ClientMessage::Auth(_) => encode_error_response(
            ErrorResponseKind::Unknown,
            ERR_INVALID_MESSAGE_TYPE,
            "auth request is only valid during handshake",
        ),
    };

    WS_HANDLE_MESSAGE_SECONDS
        .with_label_values(&[kind])
        .observe(handle_started.elapsed().as_secs_f64());
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batched_broker_config_default() {
        let config = BrokerConfig::default();
        assert_eq!(config.bind_addr.port(), 9000);
        assert_eq!(config.flush_interval, Duration::from_millis(100));
    }

    #[test]
    fn test_encode_error_response_shapes() {
        let append = encode_error_response(ErrorResponseKind::Append, 123, "append error");
        let (produce_msg, produce_len) = flux_wire::decode_server_message(&append).unwrap();
        assert_eq!(produce_len, append.len());
        let produce_resp = match produce_msg {
            ServerMessage::Append(resp) => resp,
            _ => panic!("expected append response"),
        };
        assert_eq!(produce_resp.append_seq.0, 0);
        assert!(!produce_resp.success);
        assert_eq!(produce_resp.error_code, 123);
        assert_eq!(produce_resp.error_message, "append error");
        assert!(produce_resp.append_acks.is_empty());

        let read = encode_error_response(ErrorResponseKind::Read, 124, "read error");
        let (fetch_msg, fetch_len) = flux_wire::decode_server_message(&read).unwrap();
        assert_eq!(fetch_len, read.len());
        let fetch_resp = match fetch_msg {
            ServerMessage::Read(resp) => resp,
            _ => panic!("expected read response"),
        };
        assert!(!fetch_resp.success);
        assert_eq!(fetch_resp.error_code, 124);
        assert_eq!(fetch_resp.error_message, "read error");
        assert!(fetch_resp.results.is_empty());

        let join = encode_error_response(ErrorResponseKind::JoinGroup, 125, "join error");
        let (join_msg, join_len) = flux_wire::decode_server_message(&join).unwrap();
        assert_eq!(join_len, join.len());
        let join_resp = match join_msg {
            ServerMessage::JoinGroup(resp) => resp,
            _ => panic!("expected join response"),
        };
        assert!(!join_resp.success);
        assert_eq!(join_resp.error_code, 125);
        assert_eq!(join_resp.error_message, "join error");

        let commit = encode_error_response(ErrorResponseKind::Commit, 126, "commit error");
        let (commit_msg, commit_len) = flux_wire::decode_server_message(&commit).unwrap();
        assert_eq!(commit_len, commit.len());
        let commit_resp = match commit_msg {
            ServerMessage::Commit(resp) => resp,
            _ => panic!("expected commit response"),
        };
        assert!(!commit_resp.success);
        assert_eq!(commit_resp.error_code, 126);
        assert_eq!(commit_resp.error_message, "commit error");
    }
}
