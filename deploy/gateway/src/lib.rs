use std::{
    collections::{HashMap, HashSet},
    env,
    ffi::CString,
    io, mem,
    net::SocketAddr,
    path::{Component, Path, PathBuf},
    pin::Pin,
    str::FromStr,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use axum::{
    Router,
    body::Body,
    extract::{Request, State},
    http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{any, get, post},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use futures_util::{Stream, StreamExt as _};
use hmac::{Hmac, Mac};
use hyper::{body::Incoming, server::conn::http1};
use hyper_util::{rt::TokioIo, service::TowerToHyperService};
use percent_encoding::percent_decode_str;
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt as _, AsyncSeekExt as _, AsyncWriteExt as _};
use tokio_util::io::ReaderStream;
use tower::ServiceExt as _;

const SNAPSHOT_SCHEMA_VERSION: u32 = 1;
const MAX_SNAPSHOT_BYTES: usize = 16 * 1024 * 1024;
const MAX_BYTE_RANGES: usize = 16;
const OFFICIAL_MAX_REQUEST_HEAD_BYTES: usize = 1 << 18;
const MAX_DRAIN_REQUEST_BYTES: usize = 1024;
const MAX_OFFICIAL_DRAIN_RESPONSE_BYTES: usize = 1024;
// Response observation is byte-bounded and fail-open. This accommodates the
// largest legal Bot API 10.2 Message/RichMessage result while the upstream
// response itself is always streamed unchanged.
const MAX_OUTBOUND_RESPONSE_BYTES: usize = 512 * 1024;
const MAX_OUTBOUND_PAYLOAD_BYTES: usize = 448 * 1024;
const MAX_OUTBOUND_PAYLOAD_DEPTH: usize = 64;
const MAX_OUTBOUND_OBJECT_FIELDS: usize = 512;
const MAX_OUTBOUND_ARRAY_ITEMS: usize = 512;
const MAX_CONCURRENT_OUTBOUND_OBSERVATIONS: usize = 8;
const MAX_TELEMETRY_BATCH_BYTES: usize = 512 * 1024;
const MAX_TELEMETRY_QUEUE_BYTES: usize = 8 * 1024 * 1024;
const MAX_TELEMETRY_QUEUE_EVENTS: usize = 1024;
const ROUTE_GENERATION_HEADER: &str = "x-phenogram-route-generation";
const OBSERVATION_BYPASS_HEADER: &str = "x-phenogram-observation-bypass";

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone)]
pub struct Config {
    pub public_listen_addr: SocketAddr,
    pub admin_listen_addr: SocketAddr,
    pub standard_upstream: String,
    pub local_upstream: String,
    pub standard_file_upstream: String,
    pub local_file_upstream: String,
    pub standard_control_url: Url,
    pub local_control_url: Url,
    pub snapshot_url: Url,
    pub snapshot_path: PathBuf,
    pub snapshot_refresh_interval: Duration,
    pub telemetry_url: Url,
    pub telemetry_queue_capacity: usize,
    pub public_id_key: String,
    pub sync_token: String,
}

impl Config {
    pub fn from_env() -> Result<Self, String> {
        let public_id_key = required("PUBLIC_ID_KEY")?;
        let sync_token = required("DATA_PLANE_SYNC_TOKEN")?;
        if public_id_key.len() < 32 {
            return Err("PUBLIC_ID_KEY must contain at least 32 characters".into());
        }
        if sync_token.len() < 32 {
            return Err("DATA_PLANE_SYNC_TOKEN must contain at least 32 characters".into());
        }

        let snapshot_url = Url::parse(&required("ROUTE_SNAPSHOT_URL")?)
            .map_err(|error| format!("ROUTE_SNAPSHOT_URL is invalid: {error}"))?;
        if !matches!(snapshot_url.scheme(), "http" | "https")
            || !snapshot_url.username().is_empty()
            || snapshot_url.password().is_some()
        {
            return Err("ROUTE_SNAPSHOT_URL must be an HTTP(S) URL without credentials".into());
        }

        Ok(Self {
            public_listen_addr: parse_addr(
                "PUBLIC_LISTEN_ADDR",
                optional("PUBLIC_LISTEN_ADDR")
                    .as_deref()
                    .unwrap_or("0.0.0.0:8080"),
            )?,
            admin_listen_addr: parse_addr(
                "ADMIN_LISTEN_ADDR",
                optional("ADMIN_LISTEN_ADDR")
                    .as_deref()
                    .unwrap_or("0.0.0.0:9090"),
            )?,
            standard_upstream: upstream("STANDARD_UPSTREAM_URL")?,
            local_upstream: upstream("LOCAL_UPSTREAM_URL")?,
            standard_file_upstream: upstream("STANDARD_FILE_UPSTREAM_URL")?,
            local_file_upstream: upstream("LOCAL_FILE_UPSTREAM_URL")?,
            standard_control_url: internal_url("STANDARD_OFFICIAL_CONTROL_URL")?,
            local_control_url: internal_url("LOCAL_OFFICIAL_CONTROL_URL")?,
            snapshot_url,
            snapshot_path: PathBuf::from(
                optional("ROUTE_SNAPSHOT_PATH")
                    .as_deref()
                    .unwrap_or("/var/lib/phenogram-gateway/routes.json"),
            ),
            snapshot_refresh_interval: Duration::from_secs(parse_u64(
                "ROUTE_REFRESH_SECONDS",
                optional("ROUTE_REFRESH_SECONDS").as_deref().unwrap_or("5"),
                1,
                300,
            )?),
            telemetry_url: internal_url("TELEMETRY_URL")?,
            telemetry_queue_capacity: parse_u64(
                "TELEMETRY_QUEUE_CAPACITY",
                optional("TELEMETRY_QUEUE_CAPACITY")
                    .as_deref()
                    .unwrap_or("4096"),
                100,
                65_536,
            )? as usize,
            public_id_key,
            sync_token,
        })
    }
}

#[derive(Clone)]
pub struct FileServerConfig {
    pub listen_addr: SocketAddr,
    pub root: PathBuf,
    pub pool: Pool,
    sync_token_digest: [u8; 32],
}

impl FileServerConfig {
    pub fn from_env_if_enabled() -> Result<Option<Self>, String> {
        let Some(mode) = optional("FILE_SERVER_MODE") else {
            return Ok(None);
        };
        let pool = match mode.as_str() {
            "standard" => Pool::Standard,
            "local" => Pool::Local,
            _ => return Err("FILE_SERVER_MODE must be standard or local".into()),
        };
        let root = PathBuf::from(required("FILE_SERVER_ROOT")?);
        if !valid_file_server_root(&root) {
            return Err("FILE_SERVER_ROOT must be a normalized absolute path below /".into());
        }
        let sync_token = required("DATA_PLANE_SYNC_TOKEN")?;
        if sync_token.len() < 32 {
            return Err("DATA_PLANE_SYNC_TOKEN must contain at least 32 characters".into());
        }
        Ok(Some(Self {
            listen_addr: parse_addr(
                "FILE_SERVER_LISTEN_ADDR",
                optional("FILE_SERVER_LISTEN_ADDR")
                    .as_deref()
                    .unwrap_or("0.0.0.0:8082"),
            )?,
            root,
            pool,
            sync_token_digest: Sha256::digest(sync_token.as_bytes()).into(),
        }))
    }
}

#[derive(Clone)]
pub struct GatewayState {
    client: Client,
    public_id_key: [u8; 32],
    standard_upstream: String,
    local_upstream: String,
    standard_file_upstream: String,
    local_file_upstream: String,
    standard_control_url: Url,
    local_control_url: Url,
    sync_token: String,
    sync_token_digest: [u8; 32],
    routes: Arc<RwLock<RouteTable>>,
    in_flight: Arc<Mutex<HashMap<String, usize>>>,
    ready: Arc<AtomicBool>,
    telemetry: tokio::sync::mpsc::Sender<TelemetryEvent>,
    telemetry_byte_budget: Arc<tokio::sync::Semaphore>,
    telemetry_metrics: Arc<TelemetryMetrics>,
    outbound_observation_slots: Arc<tokio::sync::Semaphore>,
    outbound_observation_clock_us: Arc<AtomicU64>,
}

impl GatewayState {
    pub fn new(
        config: &Config,
    ) -> Result<(Self, tokio::sync::mpsc::Receiver<TelemetryEvent>), String> {
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .pool_idle_timeout(Duration::from_secs(90))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| format!("failed to initialize HTTP client: {error}"))?;

        let (telemetry, receiver) = tokio::sync::mpsc::channel(
            config
                .telemetry_queue_capacity
                .min(MAX_TELEMETRY_QUEUE_EVENTS),
        );
        let state = Self {
            client,
            public_id_key: derive_public_id_key(&config.public_id_key),
            standard_upstream: config.standard_upstream.clone(),
            local_upstream: config.local_upstream.clone(),
            standard_file_upstream: config.standard_file_upstream.clone(),
            local_file_upstream: config.local_file_upstream.clone(),
            standard_control_url: config.standard_control_url.clone(),
            local_control_url: config.local_control_url.clone(),
            sync_token: config.sync_token.clone(),
            sync_token_digest: Sha256::digest(config.sync_token.as_bytes()).into(),
            routes: Arc::new(RwLock::new(RouteTable::default())),
            in_flight: Arc::new(Mutex::new(HashMap::new())),
            ready: Arc::new(AtomicBool::new(false)),
            telemetry,
            telemetry_byte_budget: Arc::new(tokio::sync::Semaphore::new(MAX_TELEMETRY_QUEUE_BYTES)),
            telemetry_metrics: Arc::new(TelemetryMetrics::default()),
            outbound_observation_slots: Arc::new(tokio::sync::Semaphore::new(
                MAX_CONCURRENT_OUTBOUND_OBSERVATIONS,
            )),
            outbound_observation_clock_us: Arc::new(AtomicU64::new(0)),
        };
        Ok((state, receiver))
    }

    pub async fn load_last_good_snapshot(&self, path: &Path) -> Result<bool, String> {
        let bytes = match tokio::fs::read(path).await {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(format!("failed to read route snapshot: {error}")),
        };
        if bytes.len() > MAX_SNAPSHOT_BYTES {
            return Err("persisted route snapshot exceeds the size limit".into());
        }
        let snapshot: RouteSnapshot = serde_json::from_slice(&bytes)
            .map_err(|error| format!("persisted route snapshot is invalid: {error}"))?;
        self.install(snapshot)?;
        self.ready.store(true, Ordering::Release);
        Ok(true)
    }

    fn install(&self, snapshot: RouteSnapshot) -> Result<(), String> {
        let next = RouteTable::try_from(snapshot)?;
        let mut current = self
            .routes
            .write()
            .map_err(|_| "route table lock was poisoned".to_string())?;
        validate_generation(&current, &next)?;
        *current = next;
        Ok(())
    }

    fn validate_candidate(&self, snapshot: &RouteSnapshot) -> Result<(), String> {
        let next = RouteTable::try_from(snapshot.clone())?;
        let current = self
            .routes
            .read()
            .map_err(|_| "route table lock was poisoned".to_string())?;
        validate_generation(&current, &next)
    }

    fn admit_route(
        &self,
        token: &[u8],
        test_dc: bool,
    ) -> Result<Option<(String, Pool, RouteAdmission)>, String> {
        let token_lookup_hash = bot_public_id(&self.public_id_key, token, test_dc);
        let routes = self
            .routes
            .read()
            .map_err(|_| "route table lock was poisoned".to_string())?;
        let Some(pool) = routes.routes.get(&token_lookup_hash).copied() else {
            return Ok(None);
        };
        // Increment while the route read lock is still held. A snapshot writer
        // cannot remove this route between admission and the in-flight fence.
        let mut in_flight = self
            .in_flight
            .lock()
            .map_err(|_| "in-flight route lock was poisoned".to_string())?;
        let count = in_flight.entry(token_lookup_hash.clone()).or_default();
        *count = count
            .checked_add(1)
            .ok_or_else(|| "in-flight route counter overflowed".to_string())?;
        let admission = RouteAdmission {
            token_lookup_hash: token_lookup_hash.clone(),
            snapshot_generation: routes.generation,
            in_flight: self.in_flight.clone(),
        };
        Ok(Some((token_lookup_hash, pool, admission)))
    }

    #[cfg(test)]
    fn route(&self, token: &[u8], test_dc: bool) -> Result<Option<(String, Pool)>, String> {
        let token_lookup_hash = bot_public_id(&self.public_id_key, token, test_dc);
        let routes = self
            .routes
            .read()
            .map_err(|_| "route table lock was poisoned".to_string())?;
        Ok(routes
            .routes
            .get(&token_lookup_hash)
            .copied()
            .map(|pool| (token_lookup_hash, pool)))
    }

    fn drain_observation(
        &self,
        token_lookup_hash: &str,
        minimum_snapshot_generation: u64,
    ) -> Result<DrainObservation, String> {
        // Keep the route read lock through the counter read. Admissions use
        // the same route -> counter lock order, so this is one coherent cut.
        let routes = self
            .routes
            .read()
            .map_err(|_| "route table lock was poisoned".to_string())?;
        let in_flight = self
            .in_flight
            .lock()
            .map_err(|_| "in-flight route lock was poisoned".to_string())?;
        let route_present = routes.routes.contains_key(token_lookup_hash);
        let in_flight = in_flight.get(token_lookup_hash).copied().unwrap_or(0);
        Ok(DrainObservation {
            snapshot_generation: routes.generation,
            route_present,
            in_flight,
            drained: routes.generation >= minimum_snapshot_generation
                && !route_present
                && in_flight == 0,
        })
    }

    fn generation(&self) -> u64 {
        self.routes.read().map_or(0, |routes| routes.generation)
    }

    fn record_telemetry(&self, mut event: TelemetryEvent) {
        let event_bytes = telemetry_event_json_len(&event).saturating_add(1);
        let Ok(event_bytes) = u32::try_from(event_bytes) else {
            self.telemetry_metrics
                .dropped
                .fetch_add(1, Ordering::Relaxed);
            return;
        };
        let Ok(permit) = self
            .telemetry_byte_budget
            .clone()
            .try_acquire_many_owned(event_bytes)
        else {
            self.telemetry_metrics
                .dropped
                .fetch_add(1, Ordering::Relaxed);
            return;
        };
        event.set_queue_permit(permit);
        match self.telemetry.try_send(event) {
            Ok(()) => {
                self.telemetry_metrics
                    .queued
                    .fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                self.telemetry_metrics
                    .dropped
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn next_outbound_observed_at_unix_us(&self) -> u64 {
        let wall_clock_us = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros()
            .min(u64::MAX as u128) as u64;
        let mut previous = self.outbound_observation_clock_us.load(Ordering::Relaxed);
        loop {
            let next = wall_clock_us.max(previous.saturating_add(1));
            match self.outbound_observation_clock_us.compare_exchange_weak(
                previous,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return next,
                Err(current) => previous = current,
            }
        }
    }

    fn admit_outbound_observation(
        &self,
        token_lookup_hash: String,
        pool: Pool,
        method: String,
        upstream_status: StatusCode,
    ) -> Option<OutboundObservation> {
        if !upstream_status.is_success() || !outbound_response_candidate(&method) {
            return None;
        }
        match self.outbound_observation_slots.clone().try_acquire_owned() {
            Ok(permit) => Some(OutboundObservation {
                token_lookup_hash,
                pool,
                method,
                upstream_status: upstream_status.as_u16(),
                _capture_permit: permit,
            }),
            Err(_) => {
                self.telemetry_metrics
                    .dropped
                    .fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }
}

struct RouteAdmission {
    token_lookup_hash: String,
    snapshot_generation: u64,
    in_flight: Arc<Mutex<HashMap<String, usize>>>,
}

impl Drop for RouteAdmission {
    fn drop(&mut self) {
        let mut in_flight = self
            .in_flight
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(count) = in_flight.get_mut(&self.token_lookup_hash) else {
            return;
        };
        if *count <= 1 {
            in_flight.remove(&self.token_lookup_hash);
        } else {
            *count -= 1;
        }
    }
}

struct AdmissionGuardedStream<S> {
    inner: Pin<Box<S>>,
    _admission: RouteAdmission,
}

struct OutboundObservationStream<S> {
    inner: Pin<Box<S>>,
    state: GatewayState,
    observation: Option<OutboundObservation>,
    expected_body_bytes: Option<usize>,
    captured: Vec<u8>,
    overflowed: bool,
}

impl<S> OutboundObservationStream<S> {
    fn new(
        inner: S,
        state: GatewayState,
        observation: OutboundObservation,
        expected_body_bytes: Option<usize>,
    ) -> Self {
        Self {
            inner: Box::pin(inner),
            state,
            observation: Some(observation),
            expected_body_bytes,
            captured: Vec::new(),
            overflowed: false,
        }
    }

    fn finish(&mut self) {
        let Some(observation) = self.observation.take() else {
            return;
        };
        if self.overflowed || !(200..300).contains(&observation.upstream_status) {
            return;
        }
        let body = mem::take(&mut self.captured);
        let state = self.state.clone();
        // Assign ordering at the clean response boundary, before detached JSON
        // parsing can reorder differently-sized responses. The per-process
        // logical microsecond clock also makes same-tick observations distinct.
        let observed_at_unix_us = state.next_outbound_observed_at_unix_us();
        // Parsing is deliberately detached from response delivery. There is no
        // await, retry, or storage operation on the Telegram response path.
        tokio::spawn(async move {
            for message in outbound_messages_from_response(&body) {
                state.record_telemetry(outbound_telemetry_event(
                    &observation,
                    observed_at_unix_us,
                    message,
                ));
            }
        });
    }

    fn fail(&mut self) {
        self.observation = None;
        self.captured.clear();
    }
}

impl<S> Stream for OutboundObservationStream<S>
where
    S: Stream<Item = Result<axum::body::Bytes, io::Error>>,
{
    type Item = Result<axum::body::Bytes, io::Error>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this.inner.as_mut().poll_next(context) {
            Poll::Ready(Some(Ok(chunk))) => {
                if this.observation.is_some() && !this.overflowed {
                    let remaining = MAX_OUTBOUND_RESPONSE_BYTES.saturating_sub(this.captured.len());
                    if chunk.len() <= remaining {
                        this.captured.extend_from_slice(&chunk);
                    } else {
                        this.overflowed = true;
                        this.captured.clear();
                    }
                    // Hyper can stop polling a response stream once the declared
                    // Content-Length has been yielded, without polling it once
                    // more for EOF. Finalize on that exact boundary as well as
                    // on an explicit clean EOF so observation remains reliable.
                    if !this.overflowed && this.expected_body_bytes == Some(this.captured.len()) {
                        this.finish();
                    }
                }
                Poll::Ready(Some(Ok(chunk)))
            }
            Poll::Ready(Some(Err(error))) => {
                this.fail();
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(None) => {
                this.finish();
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<S> AdmissionGuardedStream<S> {
    fn new(inner: S, admission: RouteAdmission) -> Self {
        Self {
            inner: Box::pin(inner),
            _admission: admission,
        }
    }
}

impl<S: Stream> Stream for AdmissionGuardedStream<S> {
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().inner.as_mut().poll_next(context)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DrainRequest {
    schema_version: u32,
    token_lookup_hash: String,
    minimum_snapshot_generation: u64,
    bot_token: String,
    telegram_test_dc: bool,
}

#[derive(Debug, Serialize)]
struct DrainResponse {
    schema_version: u32,
    drained: bool,
    snapshot_generation: String,
    route_present: bool,
    in_flight: String,
    official_fenced: bool,
    official_active_requests: OfficialPoolCounts,
}

#[derive(Debug, Serialize)]
struct OfficialPoolCounts {
    standard: Option<String>,
    local: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OfficialDrainProof {
    schema_version: u32,
    fenced: bool,
    telegram_bot_id: String,
    telegram_test_dc: bool,
    route_generation: String,
    active_requests: String,
}

struct DrainObservation {
    snapshot_generation: u64,
    route_present: bool,
    in_flight: usize,
    drained: bool,
}

#[derive(Serialize)]
struct OfficialDrainCommand<'a> {
    schema_version: u32,
    bot_token: &'a str,
    telegram_test_dc: bool,
    route_generation: String,
}

struct OfficialDrainResult {
    standard: Option<u64>,
    local: Option<u64>,
}

impl OfficialDrainResult {
    fn fences_armed(&self) -> bool {
        self.standard.is_some() && self.local.is_some()
    }

    fn is_idle(&self) -> bool {
        self.standard == Some(0) && self.local == Some(0)
    }

    fn response_counts(&self) -> OfficialPoolCounts {
        OfficialPoolCounts {
            standard: self.standard.map(|value| value.to_string()),
            local: self.local.map(|value| value.to_string()),
        }
    }
}

#[derive(Clone)]
pub struct FileServerState {
    root: PathBuf,
    pool: Pool,
    sync_token_digest: [u8; 32],
}

impl From<FileServerConfig> for FileServerState {
    fn from(config: FileServerConfig) -> Self {
        Self {
            root: config.root,
            pool: config.pool,
            sync_token_digest: config.sync_token_digest,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Pool {
    Standard,
    Local,
}

#[derive(Default)]
struct TelemetryMetrics {
    queued: AtomicU64,
    dropped: AtomicU64,
    delivered: AtomicU64,
    delivery_failed: AtomicU64,
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum TelemetryEvent {
    ApiCall(ApiCallTelemetryEvent),
    OutboundMessage(Box<OutboundMessageTelemetryEvent>),
}

impl TelemetryEvent {
    fn set_queue_permit(&mut self, permit: tokio::sync::OwnedSemaphorePermit) {
        match self {
            Self::ApiCall(event) => event._queue_permit = Some(permit),
            Self::OutboundMessage(event) => event._queue_permit = Some(permit),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ApiCallTelemetryEvent {
    schema_version: u32,
    token_lookup_hash: String,
    pool: Pool,
    method: String,
    upstream_status: u16,
    latency_ms: u64,
    observed_at_unix_ms: u64,
    #[serde(skip)]
    _queue_permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

#[derive(Debug, Serialize)]
pub struct OutboundMessageTelemetryEvent {
    schema_version: u32,
    kind: &'static str,
    token_lookup_hash: String,
    pool: Pool,
    method: String,
    upstream_status: u16,
    observed_at_unix_us: u64,
    message: ObservedOutboundMessage,
    #[serde(skip)]
    _queue_permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

#[derive(Debug, Serialize)]
pub struct ObservedOutboundMessage {
    chat_id: i64,
    telegram_message_id: Option<i64>,
    receiver_user_id: Option<i64>,
    ephemeral_message_id: Option<i64>,
    business_connection_id: Option<String>,
    guest_query_id: Option<String>,
    message_thread_id: Option<i64>,
    direct_messages_topic_id: Option<i64>,
    text: Option<String>,
    chat_type: Option<String>,
    title: Option<String>,
    username: Option<String>,
    display_name: Option<String>,
    payload: Option<Box<serde_json::Value>>,
}

struct OutboundObservation {
    token_lookup_hash: String,
    pool: Pool,
    method: String,
    upstream_status: u16,
    _capture_permit: tokio::sync::OwnedSemaphorePermit,
}

#[derive(Serialize)]
struct TelemetryBatch<'a> {
    schema_version: u32,
    events: &'a [TelemetryEvent],
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RouteRecord {
    pub token_lookup_hash: String,
    pub pool: Pool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RouteSnapshot {
    pub schema_version: u32,
    pub generation: u64,
    pub routes: Vec<RouteRecord>,
}

#[derive(Default)]
struct RouteTable {
    initialized: bool,
    generation: u64,
    routes: HashMap<String, Pool>,
}

impl TryFrom<RouteSnapshot> for RouteTable {
    type Error = String;

    fn try_from(snapshot: RouteSnapshot) -> Result<Self, Self::Error> {
        if snapshot.schema_version != SNAPSHOT_SCHEMA_VERSION {
            return Err(format!(
                "unsupported route snapshot schema version {}",
                snapshot.schema_version
            ));
        }
        let mut routes = HashMap::with_capacity(snapshot.routes.len());
        for route in snapshot.routes {
            if !valid_token_lookup_hash(&route.token_lookup_hash) {
                return Err("route snapshot contains an invalid token_lookup_hash".into());
            }
            if routes.insert(route.token_lookup_hash, route.pool).is_some() {
                return Err("route snapshot contains a duplicate token_lookup_hash".into());
            }
        }
        Ok(Self {
            initialized: true,
            generation: snapshot.generation,
            routes,
        })
    }
}

fn validate_generation(current: &RouteTable, next: &RouteTable) -> Result<(), String> {
    if !current.initialized {
        return Ok(());
    }
    if next.generation < current.generation {
        return Err("route snapshot generation moved backwards".into());
    }
    if next.generation == current.generation && next.routes != current.routes {
        return Err("route snapshot content changed without a new generation".into());
    }
    Ok(())
}

pub fn public_router(state: GatewayState) -> Router {
    Router::new().fallback(any(proxy)).with_state(state)
}

pub fn file_server_router(state: FileServerState) -> Router {
    Router::new().fallback(any(serve_file)).with_state(state)
}

pub fn admin_router(state: GatewayState) -> Router {
    Router::new()
        .route("/health/live", get(live))
        .route("/health/ready", get(ready))
        .route("/internal/routes/drain", post(drain_route))
        .with_state(state)
}

pub async fn serve_public_http1(
    listener: tokio::net::TcpListener,
    router: Router,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> io::Result<()> {
    let mut connections = tokio::task::JoinSet::new();

    loop {
        let accepted = tokio::select! {
            result = listener.accept() => Some(Some(result)),
            _ = connections.join_next(), if !connections.is_empty() => Some(None),
            _ = shutdown_requested(&mut shutdown) => None,
        };
        let Some(accepted) = accepted else {
            break;
        };
        let Some(accepted) = accepted else {
            continue;
        };
        let (stream, _) = match accepted {
            Ok(connection) => connection,
            Err(error) => {
                // Accept failures carry no request data. Retry so a transient
                // descriptor-pressure event does not terminate the gateway.
                tracing::warn!(reason = %error, "public listener accept failed; retrying");
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                    _ = shutdown_requested(&mut shutdown) => break,
                }
                continue;
            }
        };

        let tower_service = router
            .clone()
            .map_request(|request: hyper::Request<Incoming>| request.map(Body::new));
        let hyper_service = TowerToHyperService::new(tower_service);
        let mut connection_shutdown = shutdown.clone();
        connections.spawn(async move {
            let mut builder = http1::Builder::new();
            builder
                .max_buf_size(OFFICIAL_MAX_REQUEST_HEAD_BYTES)
                .header_read_timeout(None);
            let connection = builder.serve_connection(TokioIo::new(stream), hyper_service);
            tokio::pin!(connection);
            tokio::select! {
                _ = connection.as_mut() => {}
                _ = shutdown_requested(&mut connection_shutdown) => {
                    connection.as_mut().graceful_shutdown();
                    let _ = connection.await;
                }
            }
        });
    }

    drop(listener);
    while connections.join_next().await.is_some() {}
    Ok(())
}

async fn shutdown_requested(receiver: &mut tokio::sync::watch::Receiver<bool>) {
    while !*receiver.borrow() && receiver.changed().await.is_ok() {}
}

pub async fn snapshot_sync_loop(state: GatewayState, config: Config) {
    loop {
        if let Err(error) = sync_snapshot(&state, &config).await {
            // Never include the URL, response body, token, or bearer secret in logs.
            tracing::warn!(reason = %error, "route snapshot refresh failed; retaining last-good routes");
        }
        tokio::time::sleep(config.snapshot_refresh_interval).await;
    }
}

pub async fn telemetry_delivery_loop(
    state: GatewayState,
    config: Config,
    mut receiver: tokio::sync::mpsc::Receiver<TelemetryEvent>,
) {
    const MAX_BATCH: usize = 100;
    const MAX_BATCH_WAIT: Duration = Duration::from_millis(250);
    const BATCH_WRAPPER_BYTES: usize = 40;

    let mut pending = None;
    loop {
        let first = match pending.take() {
            Some(event) => event,
            None => match receiver.recv().await {
                Some(event) => event,
                None => break,
            },
        };
        let mut events = Vec::with_capacity(MAX_BATCH);
        let mut serialized_bytes = BATCH_WRAPPER_BYTES + telemetry_event_json_len(&first);
        events.push(first);
        let deadline = tokio::time::Instant::now() + MAX_BATCH_WAIT;
        while events.len() < MAX_BATCH {
            match tokio::time::timeout_at(deadline, receiver.recv()).await {
                Ok(Some(event)) => {
                    let event_bytes = telemetry_event_json_len(&event) + 1;
                    if serialized_bytes + event_bytes > MAX_TELEMETRY_BATCH_BYTES {
                        pending = Some(event);
                        break;
                    }
                    serialized_bytes += event_bytes;
                    events.push(event);
                }
                Ok(None) | Err(_) => break,
            }
        }
        let count = events.len() as u64;
        let mut retry_delay = Duration::from_millis(100);
        loop {
            let result = state
                .client
                .post(config.telemetry_url.clone())
                .bearer_auth(&config.sync_token)
                .timeout(Duration::from_secs(2))
                .json(&TelemetryBatch {
                    schema_version: 1,
                    events: &events,
                })
                .send()
                .await;
            let retryable = match result {
                Ok(response) if response.status().is_success() => {
                    state
                        .telemetry_metrics
                        .delivered
                        .fetch_add(count, Ordering::Relaxed);
                    break;
                }
                Ok(response) => {
                    response.status() == StatusCode::TOO_MANY_REQUESTS
                        || response.status().is_server_error()
                }
                Err(_) => true,
            };
            state
                .telemetry_metrics
                .delivery_failed
                .fetch_add(count, Ordering::Relaxed);
            if !retryable {
                break;
            }
            tokio::time::sleep(retry_delay).await;
            retry_delay = retry_delay.saturating_mul(2).min(Duration::from_secs(5));
        }
    }
}

fn telemetry_event_json_len(event: &TelemetryEvent) -> usize {
    serde_json::to_vec(event).map_or(MAX_TELEMETRY_BATCH_BYTES, |body| body.len())
}

async fn sync_snapshot(state: &GatewayState, config: &Config) -> Result<(), String> {
    let response = state
        .client
        .get(config.snapshot_url.clone())
        .bearer_auth(&config.sync_token)
        .header(header::ACCEPT, "application/json")
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .map_err(|_| "control plane is unreachable".to_string())?;

    if response.status() == StatusCode::NOT_MODIFIED {
        return Ok(());
    }
    if response.status() != StatusCode::OK {
        return Err(format!(
            "control plane returned HTTP {}",
            response.status().as_u16()
        ));
    }
    if response
        .content_length()
        .is_some_and(|length| length > MAX_SNAPSHOT_BYTES as u64)
    {
        return Err("route snapshot exceeds the size limit".into());
    }
    let bytes = response
        .bytes()
        .await
        .map_err(|_| "route snapshot body could not be read".to_string())?;
    if bytes.len() > MAX_SNAPSHOT_BYTES {
        return Err("route snapshot exceeds the size limit".into());
    }
    let snapshot: RouteSnapshot =
        serde_json::from_slice(&bytes).map_err(|_| "route snapshot JSON is invalid".to_string())?;
    state.validate_candidate(&snapshot)?;

    persist_snapshot(&config.snapshot_path, &bytes).await?;
    state.install(snapshot)?;
    state.ready.store(true, Ordering::Release);
    Ok(())
}

async fn persist_snapshot(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let parent = path
        .parent()
        .ok_or_else(|| "ROUTE_SNAPSHOT_PATH must have a parent directory".to_string())?;
    tokio::fs::create_dir_all(parent)
        .await
        .map_err(|error| format!("failed to create snapshot directory: {error}"))?;
    let temporary = path.with_extension(format!("tmp-{}", std::process::id()));
    let mut options = tokio::fs::OpenOptions::new();
    options.create(true).truncate(true).write(true);
    let mut file = options
        .open(&temporary)
        .await
        .map_err(|error| format!("failed to create route snapshot: {error}"))?;
    file.write_all(bytes)
        .await
        .map_err(|error| format!("failed to write route snapshot: {error}"))?;
    file.sync_all()
        .await
        .map_err(|error| format!("failed to sync route snapshot: {error}"))?;
    drop(file);
    tokio::fs::rename(&temporary, path)
        .await
        .map_err(|error| format!("failed to install route snapshot: {error}"))?;
    let parent = parent.to_owned();
    tokio::task::spawn_blocking(move || std::fs::File::open(parent)?.sync_all())
        .await
        .map_err(|error| format!("snapshot directory sync task failed: {error}"))?
        .map_err(|error| format!("failed to sync snapshot directory: {error}"))?;
    Ok(())
}

async fn proxy(State(state): State<GatewayState>, request: Request) -> Response {
    if request.uri().scheme().is_some() || !request.uri().path().starts_with('/') {
        return telegram_error(
            StatusCode::NOT_FOUND,
            "Not Found: absolute URI is specified in the Request-Line",
        );
    }
    let decoded_path = percent_decode_str(request.uri().path()).collect::<Vec<_>>();
    let Some(parsed_path) = parse_public_request_path(&decoded_path) else {
        return telegram_error(StatusCode::NOT_FOUND, "Not Found");
    };
    let method = telegram_method(parsed_path);
    let (token_lookup_hash, pool, admission) = match state
        .admit_route(parsed_path.token, parsed_path.test_dc)
    {
        Ok(Some(route)) => route,
        Ok(None) => return telegram_error(StatusCode::UNAUTHORIZED, "Unauthorized"),
        Err(_) => return telegram_error(StatusCode::SERVICE_UNAVAILABLE, "Service Unavailable"),
    };
    if matches!(parsed_path.kind, PublicPathKind::File { .. }) {
        return proxy_file(&state, request, token_lookup_hash, pool, method, admission).await;
    }
    let upstream = match pool {
        Pool::Standard => &state.standard_upstream,
        Pool::Local => &state.local_upstream,
    };
    let path_and_query = request
        .uri()
        .path_and_query()
        .map_or_else(|| request.uri().path(), |value| value.as_str());
    let upstream_url = format!("{upstream}{path_and_query}");

    let (parts, body) = request.into_parts();
    let suppress_outbound_observation = valid_secret_header(
        &parts.headers,
        OBSERVATION_BYPASS_HEADER,
        &state.sync_token_digest,
    );
    let mut builder = state.client.request(parts.method, upstream_url);
    let connection_headers = connection_nominated_headers(&parts.headers);
    for (name, value) in &parts.headers {
        if !hop_by_hop(name)
            && !connection_headers.contains(name)
            && name != header::HOST
            && name.as_str() != ROUTE_GENERATION_HEADER
            && name.as_str() != OBSERVATION_BYPASS_HEADER
        {
            builder = builder.header(name, value);
        }
    }
    // This marker is generated only after admission under the route-table read
    // lock. Never forward a caller-supplied value.
    builder = builder.header(
        ROUTE_GENERATION_HEADER,
        admission.snapshot_generation.to_string(),
    );
    let body = reqwest::Body::wrap_stream(body.into_data_stream());
    let started = Instant::now();
    let response = match builder.body(body).send().await {
        Ok(response) => response,
        Err(_) => {
            state.record_telemetry(telemetry_event(
                token_lookup_hash,
                pool,
                method,
                StatusCode::BAD_GATEWAY,
                started.elapsed(),
            ));
            return telegram_error(StatusCode::BAD_GATEWAY, "Bad Gateway");
        }
    };

    let status = response.status();
    state.record_telemetry(telemetry_event(
        token_lookup_hash.clone(),
        pool,
        method.clone(),
        status,
        started.elapsed(),
    ));
    let observation = (!suppress_outbound_observation)
        .then(|| state.admit_outbound_observation(token_lookup_hash, pool, method, status))
        .flatten();
    let expected_body_bytes = response
        .content_length()
        .and_then(|length| usize::try_from(length).ok());
    let headers = response.headers().clone();
    let stream = response
        .bytes_stream()
        .map(|chunk| chunk.map_err(|_| std::io::Error::other("upstream body stream failed")));
    let body = match observation {
        Some(observation) => Body::from_stream(AdmissionGuardedStream::new(
            OutboundObservationStream::new(stream, state.clone(), observation, expected_body_bytes),
            admission,
        )),
        None => Body::from_stream(AdmissionGuardedStream::new(stream, admission)),
    };
    let mut downstream = Response::builder().status(status);
    if let Some(target) = downstream.headers_mut() {
        copy_end_to_end_headers(&headers, target);
    }
    downstream
        .body(body)
        .unwrap_or_else(|_| telegram_error(StatusCode::BAD_GATEWAY, "Bad Gateway"))
}

async fn proxy_file(
    state: &GatewayState,
    request: Request,
    token_lookup_hash: String,
    pool: Pool,
    method_name: String,
    admission: RouteAdmission,
) -> Response {
    if request.method() != Method::GET && request.method() != Method::HEAD {
        let status = StatusCode::METHOD_NOT_ALLOWED;
        state.record_telemetry(telemetry_event(
            token_lookup_hash,
            pool,
            method_name,
            status,
            Duration::ZERO,
        ));
        let mut response = empty_file_response(status);
        response
            .headers_mut()
            .insert(header::ALLOW, HeaderValue::from_static("GET, HEAD"));
        return response;
    }

    let file_upstream = match pool {
        Pool::Standard => &state.standard_file_upstream,
        Pool::Local => &state.local_file_upstream,
    };
    let upstream_url = format!("{file_upstream}{}", request.uri().path());
    let mut builder = state
        .client
        .request(request.method().clone(), upstream_url)
        .bearer_auth(&state.sync_token);
    if let Some(range) = request.headers().get(header::RANGE) {
        builder = builder.header(header::RANGE, range);
    }

    let started = Instant::now();
    let response = match builder.send().await {
        Ok(response) => response,
        Err(_) => {
            state.record_telemetry(telemetry_event(
                token_lookup_hash,
                pool,
                method_name,
                StatusCode::BAD_GATEWAY,
                started.elapsed(),
            ));
            return empty_file_response(StatusCode::BAD_GATEWAY);
        }
    };
    let status = response.status();
    state.record_telemetry(telemetry_event(
        token_lookup_hash,
        pool,
        method_name,
        status,
        started.elapsed(),
    ));
    let headers = response.headers().clone();
    let stream = response
        .bytes_stream()
        .map(|chunk| chunk.map_err(|_| io::Error::other("file sidecar body stream failed")));
    let stream = AdmissionGuardedStream::new(stream, admission);
    let mut downstream = Response::builder().status(status);
    if let Some(target) = downstream.headers_mut() {
        copy_end_to_end_headers(&headers, target);
    }
    downstream
        .body(Body::from_stream(stream))
        .unwrap_or_else(|_| empty_file_response(StatusCode::BAD_GATEWAY))
}

#[derive(Clone)]
struct FileRequestPath {
    token: Vec<u8>,
    test_dc: bool,
    file_path: Vec<u8>,
}

#[derive(Clone, Copy)]
struct ParsedPublicPath<'a> {
    token: &'a [u8],
    test_dc: bool,
    kind: PublicPathKind<'a>,
}

#[derive(Clone, Copy)]
enum PublicPathKind<'a> {
    Api { method: &'a [u8] },
    File { file_path: &'a [u8] },
}

struct OpenedFile {
    file: tokio::fs::File,
    length: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ByteRange {
    start: u64,
    end: u64,
}

#[derive(Debug)]
enum SecureOpenError {
    NotFound,
    Unavailable,
}

async fn serve_file(State(state): State<FileServerState>, request: Request) -> Response {
    if !valid_internal_authorization(request.headers(), &state.sync_token_digest) {
        let mut response = empty_file_response(StatusCode::UNAUTHORIZED);
        response
            .headers_mut()
            .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
        return response;
    }
    if request.method() != Method::GET && request.method() != Method::HEAD {
        let mut response = empty_file_response(StatusCode::METHOD_NOT_ALLOWED);
        response
            .headers_mut()
            .insert(header::ALLOW, HeaderValue::from_static("GET, HEAD"));
        return response;
    }
    let Some(file_request) = parse_file_request_path(request.uri().path()) else {
        return empty_file_response(StatusCode::NOT_FOUND);
    };
    let range = request.headers().get(header::RANGE).cloned();
    let is_head = request.method() == Method::HEAD;
    let opened = match open_requested_file(&state, file_request).await {
        Ok(file) => file,
        Err(SecureOpenError::NotFound) => return empty_file_response(StatusCode::NOT_FOUND),
        Err(SecureOpenError::Unavailable) => {
            return empty_file_response(StatusCode::SERVICE_UNAVAILABLE);
        }
    };
    file_response(opened, range.as_ref(), is_head).await
}

fn valid_internal_authorization(headers: &HeaderMap, expected_digest: &[u8; 32]) -> bool {
    let Some(value) = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
    else {
        return false;
    };
    secret_matches_digest(value.as_bytes(), expected_digest)
}

fn valid_secret_header(
    headers: &HeaderMap,
    name: &'static str,
    expected_digest: &[u8; 32],
) -> bool {
    headers
        .get(name)
        .is_some_and(|value| secret_matches_digest(value.as_bytes(), expected_digest))
}

fn secret_matches_digest(value: &[u8], expected_digest: &[u8; 32]) -> bool {
    let presented: [u8; 32] = Sha256::digest(value).into();
    presented
        .iter()
        .zip(expected_digest)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

fn parse_file_request_path(path: &str) -> Option<FileRequestPath> {
    let decoded = percent_decode_str(path).collect::<Vec<_>>();
    let parsed = parse_decoded_file_request_path(&decoded)?;
    Some(parsed.into_owned_file_request())
}

async fn open_requested_file(
    state: &FileServerState,
    request: FileRequestPath,
) -> Result<OpenedFile, SecureOpenError> {
    let root = state.root.clone();
    let pool = state.pool;
    let result = tokio::task::spawn_blocking(move || {
        let requested_path = decode_file_path(&request.file_path)?;
        let bot_directories = native_bot_directories(&request.token, request.test_dc)?;
        match pool {
            Pool::Standard => {
                if requested_path.is_absolute() {
                    return Err(SecureOpenError::NotFound);
                }
                validate_media_relative_path(&requested_path)?;
                for bot_directory in bot_directories {
                    let relative = bot_directory.join(&requested_path);
                    match secure_open_relative(&root, &relative) {
                        Ok(file) => return Ok(file),
                        Err(SecureOpenError::NotFound) => {}
                        Err(error) => return Err(error),
                    }
                }
                Err(SecureOpenError::NotFound)
            }
            Pool::Local => {
                let relative = if requested_path.is_absolute() {
                    requested_path
                        .strip_prefix(&root)
                        .map_err(|_| SecureOpenError::NotFound)?
                } else {
                    // ingress-nginx normally preserves the double slash before
                    // an absolute local-mode path. If an intermediary merges
                    // it, accept only the exact configured root without its
                    // leading slash; arbitrary relative local paths stay invalid.
                    validate_relative_path(&requested_path)?;
                    let root_without_leading_slash = root
                        .strip_prefix(Path::new("/"))
                        .map_err(|_| SecureOpenError::NotFound)?;
                    if root_without_leading_slash.as_os_str().is_empty() {
                        return Err(SecureOpenError::NotFound);
                    }
                    requested_path
                        .strip_prefix(root_without_leading_slash)
                        .map_err(|_| SecureOpenError::NotFound)?
                };
                validate_relative_path(relative)?;
                let first = relative
                    .components()
                    .next()
                    .and_then(|component| match component {
                        Component::Normal(value) => Some(value),
                        _ => None,
                    })
                    .ok_or(SecureOpenError::NotFound)?;
                if !bot_directories
                    .iter()
                    .any(|bot_directory| first == bot_directory.as_os_str())
                {
                    return Err(SecureOpenError::NotFound);
                }
                let media_directory = relative
                    .components()
                    .nth(1)
                    .and_then(|component| match component {
                        Component::Normal(value) => Some(value),
                        _ => None,
                    })
                    .ok_or(SecureOpenError::NotFound)?;
                if !is_telegram_media_directory(media_directory) {
                    return Err(SecureOpenError::NotFound);
                }
                secure_open_relative(&root, relative)
            }
        }
    })
    .await
    .map_err(|_| SecureOpenError::Unavailable)??;
    Ok(OpenedFile {
        file: tokio::fs::File::from_std(result.0),
        length: result.1,
    })
}

#[cfg(unix)]
fn decode_file_path(decoded: &[u8]) -> Result<PathBuf, SecureOpenError> {
    use std::{ffi::OsStr, os::unix::ffi::OsStrExt};

    Ok(PathBuf::from(OsStr::from_bytes(decoded)))
}

#[cfg(not(unix))]
fn decode_file_path(decoded: &[u8]) -> Result<PathBuf, SecureOpenError> {
    std::str::from_utf8(decoded)
        .map(PathBuf::from)
        .map_err(|_| SecureOpenError::NotFound)
}

fn validate_relative_path(path: &Path) -> Result<(), SecureOpenError> {
    let mut count = 0_usize;
    for component in path.components() {
        match component {
            Component::Normal(value) if !value.is_empty() => count += 1,
            _ => return Err(SecureOpenError::NotFound),
        }
    }
    if count == 0 {
        return Err(SecureOpenError::NotFound);
    }
    Ok(())
}

// Pinned TDLib stores its downloaded Bot API files below these file-type
// directories. The bot directory also contains TDLib databases and session
// state, which must never become downloadable through `/file/bot...` even to
// a caller holding the bot token.
const TELEGRAM_MEDIA_DIRECTORIES: &[&str] = &[
    "thumbnails",
    "profile_photos",
    "photos",
    "voice",
    "videos",
    "documents",
    "secret",
    "temp",
    "stickers",
    "music",
    "animations",
    "secret_thumbnails",
    "video_notes",
    "passport",
    "wallpapers",
    "notification_sounds",
    "stories",
];

fn is_telegram_media_directory(component: &std::ffi::OsStr) -> bool {
    component
        .to_str()
        .is_some_and(|name| TELEGRAM_MEDIA_DIRECTORIES.contains(&name))
}

fn validate_media_relative_path(path: &Path) -> Result<(), SecureOpenError> {
    validate_relative_path(path)?;
    let first = path
        .components()
        .next()
        .and_then(|component| match component {
            Component::Normal(value) => Some(value),
            _ => None,
        })
        .ok_or(SecureOpenError::NotFound)?;
    if !is_telegram_media_directory(first) {
        return Err(SecureOpenError::NotFound);
    }
    Ok(())
}

fn valid_file_server_root(path: &Path) -> bool {
    let mut components = path.components();
    if !matches!(components.next(), Some(Component::RootDir)) {
        return false;
    }
    let mut normal_components = 0_usize;
    for component in components {
        match component {
            Component::Normal(value) if !value.is_empty() => normal_components += 1,
            _ => return false,
        }
    }
    normal_components > 0
}

fn native_bot_directories(token: &[u8], test_dc: bool) -> Result<Vec<PathBuf>, SecureOpenError> {
    if token.is_empty() {
        return Err(SecureOpenError::NotFound);
    }
    let mut native = token.to_vec();
    if test_dc {
        native.extend_from_slice(b":T");
    }
    let fallback = native
        .iter()
        .map(|byte| if *byte == b':' { b'~' } else { *byte })
        .collect::<Vec<_>>();
    if fallback == native {
        Ok(vec![path_from_bytes(native)?])
    } else {
        Ok(vec![path_from_bytes(native)?, path_from_bytes(fallback)?])
    }
}

#[cfg(unix)]
fn path_from_bytes(bytes: Vec<u8>) -> Result<PathBuf, SecureOpenError> {
    use std::{ffi::OsString, os::unix::ffi::OsStringExt};

    Ok(PathBuf::from(OsString::from_vec(bytes)))
}

#[cfg(not(unix))]
fn path_from_bytes(bytes: Vec<u8>) -> Result<PathBuf, SecureOpenError> {
    String::from_utf8(bytes)
        .map(PathBuf::from)
        .map_err(|_| SecureOpenError::NotFound)
}

#[cfg(unix)]
fn secure_open_relative(
    root: &Path,
    relative: &Path,
) -> Result<(std::fs::File, u64), SecureOpenError> {
    use std::os::{
        fd::{AsRawFd, FromRawFd, OwnedFd},
        unix::ffi::OsStrExt,
    };

    validate_relative_path(relative)?;
    let root_path =
        CString::new(root.as_os_str().as_bytes()).map_err(|_| SecureOpenError::Unavailable)?;
    let root_fd = unsafe {
        libc::open(
            root_path.as_ptr(),
            libc::O_RDONLY
                | libc::O_DIRECTORY
                | libc::O_CLOEXEC
                | libc::O_NOFOLLOW
                | libc::O_NONBLOCK,
        )
    };
    if root_fd < 0 {
        return Err(SecureOpenError::Unavailable);
    }
    let mut current = unsafe { OwnedFd::from_raw_fd(root_fd) };
    let root_stat = stat_fd(current.as_raw_fd()).map_err(|_| SecureOpenError::Unavailable)?;
    let components = relative.components().collect::<Vec<_>>();

    for (index, component) in components.iter().enumerate() {
        let Component::Normal(value) = component else {
            return Err(SecureOpenError::NotFound);
        };
        let name = CString::new(value.as_bytes()).map_err(|_| SecureOpenError::NotFound)?;
        let is_final = index + 1 == components.len();
        let mut flags = libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK;
        if !is_final {
            flags |= libc::O_DIRECTORY;
        }
        let next_fd = unsafe { libc::openat(current.as_raw_fd(), name.as_ptr(), flags) };
        if next_fd < 0 {
            return Err(classify_component_open_error(io::Error::last_os_error()));
        }
        let next = unsafe { OwnedFd::from_raw_fd(next_fd) };
        let next_stat = stat_fd(next.as_raw_fd()).map_err(classify_component_open_error)?;
        if next_stat.st_dev != root_stat.st_dev {
            return Err(SecureOpenError::NotFound);
        }
        if is_final {
            if next_stat.st_mode & libc::S_IFMT != libc::S_IFREG || next_stat.st_size < 0 {
                return Err(SecureOpenError::NotFound);
            }
            let length = next_stat.st_size as u64;
            return Ok((std::fs::File::from(next), length));
        }
        current = next;
    }
    Err(SecureOpenError::NotFound)
}

#[cfg(unix)]
fn stat_fd(fd: std::os::fd::RawFd) -> io::Result<libc::stat> {
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    let result = unsafe { libc::fstat(fd, stat.as_mut_ptr()) };
    if result == 0 {
        Ok(unsafe { stat.assume_init() })
    } else {
        Err(io::Error::last_os_error())
    }
}

fn classify_component_open_error(error: io::Error) -> SecureOpenError {
    match error.raw_os_error() {
        Some(libc::EMFILE | libc::ENFILE | libc::ENOMEM | libc::EIO) => {
            SecureOpenError::Unavailable
        }
        _ => SecureOpenError::NotFound,
    }
}

#[cfg(not(unix))]
fn secure_open_relative(
    _root: &Path,
    _relative: &Path,
) -> Result<(std::fs::File, u64), SecureOpenError> {
    Err(SecureOpenError::Unavailable)
}

async fn file_response(opened: OpenedFile, range: Option<&HeaderValue>, head: bool) -> Response {
    let ranges = match range.map(|value| parse_byte_ranges(value, opened.length)) {
        None | Some(Ok(None)) => None,
        Some(Ok(Some(ranges))) => Some(ranges),
        Some(Err(())) => return range_not_satisfiable(opened.length),
    };
    match ranges {
        None => full_file_response(opened, head),
        Some(ranges) if ranges.len() == 1 => single_range_response(opened, ranges[0], head).await,
        Some(ranges) => multi_range_response(opened, ranges, head).await,
    }
}

fn full_file_response(opened: OpenedFile, head: bool) -> Response {
    let body = if head {
        Body::empty()
    } else {
        Body::from_stream(ReaderStream::new(opened.file))
    };
    build_file_response(StatusCode::OK, opened.length, None, None, body)
}

async fn single_range_response(mut opened: OpenedFile, range: ByteRange, head: bool) -> Response {
    let length = range.end - range.start + 1;
    if !head
        && opened
            .file
            .seek(std::io::SeekFrom::Start(range.start))
            .await
            .is_err()
    {
        return empty_file_response(StatusCode::SERVICE_UNAVAILABLE);
    }
    let content_range = format!("bytes {}-{}/{}", range.start, range.end, opened.length);
    let body = if head {
        Body::empty()
    } else {
        Body::from_stream(ReaderStream::new(opened.file.take(length)))
    };
    build_file_response(
        StatusCode::PARTIAL_CONTENT,
        length,
        Some(content_range),
        None,
        body,
    )
}

async fn multi_range_response(
    mut opened: OpenedFile,
    ranges: Vec<ByteRange>,
    head: bool,
) -> Response {
    let boundary = format!(
        "phenogram-{:x}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    );
    let part_headers = ranges
        .iter()
        .map(|range| {
            format!(
                "--{boundary}\r\nContent-Type: application/octet-stream\r\nContent-Range: bytes {}-{}/{}\r\n\r\n",
                range.start, range.end, opened.length
            )
            .into_bytes()
        })
        .collect::<Vec<_>>();
    let closing = format!("--{boundary}--\r\n").into_bytes();
    let Some(content_length) = multipart_content_length(&part_headers, &ranges, closing.len())
    else {
        return empty_file_response(StatusCode::SERVICE_UNAVAILABLE);
    };
    let content_type = format!("multipart/byteranges; boundary={boundary}");
    if head {
        return build_file_response(
            StatusCode::PARTIAL_CONTENT,
            content_length,
            None,
            Some(content_type),
            Body::empty(),
        );
    }

    let (mut writer, reader) = tokio::io::duplex(64 * 1024);
    tokio::spawn(async move {
        for (header_bytes, range) in part_headers.into_iter().zip(ranges) {
            if writer.write_all(&header_bytes).await.is_err()
                || opened
                    .file
                    .seek(std::io::SeekFrom::Start(range.start))
                    .await
                    .is_err()
            {
                return;
            }
            let mut limited = (&mut opened.file).take(range.end - range.start + 1);
            if tokio::io::copy(&mut limited, &mut writer).await.is_err()
                || writer.write_all(b"\r\n").await.is_err()
            {
                return;
            }
        }
        let _ = writer.write_all(&closing).await;
    });
    build_file_response(
        StatusCode::PARTIAL_CONTENT,
        content_length,
        None,
        Some(content_type),
        Body::from_stream(ReaderStream::new(reader)),
    )
}

fn multipart_content_length(
    headers: &[Vec<u8>],
    ranges: &[ByteRange],
    closing_length: usize,
) -> Option<u64> {
    headers
        .iter()
        .zip(ranges)
        .try_fold(0_u64, |total, (header, range)| {
            total
                .checked_add(header.len() as u64)?
                .checked_add(range.end - range.start + 1)?
                .checked_add(2)
        })?
        .checked_add(closing_length as u64)
}

fn parse_byte_ranges(value: &HeaderValue, file_length: u64) -> Result<Option<Vec<ByteRange>>, ()> {
    let value = value.to_str().map_err(|_| ())?;
    let Some(specifications) = value.strip_prefix("bytes=") else {
        return Ok(None);
    };
    let specifications = specifications.split(',').collect::<Vec<_>>();
    if specifications.is_empty() || specifications.len() > MAX_BYTE_RANGES {
        return Err(());
    }
    let mut ranges = Vec::with_capacity(specifications.len());
    for specification in specifications {
        let specification = specification.trim();
        let (start, end) = specification.split_once('-').ok_or(())?;
        if start.is_empty() {
            let suffix = end.parse::<u64>().map_err(|_| ())?;
            if suffix == 0 || file_length == 0 {
                continue;
            }
            ranges.push(ByteRange {
                start: file_length.saturating_sub(suffix),
                end: file_length - 1,
            });
            continue;
        }
        let start = start.parse::<u64>().map_err(|_| ())?;
        if start >= file_length {
            continue;
        }
        let end = if end.is_empty() {
            file_length - 1
        } else {
            end.parse::<u64>().map_err(|_| ())?.min(file_length - 1)
        };
        if end < start {
            return Err(());
        }
        ranges.push(ByteRange { start, end });
    }
    if ranges.is_empty() {
        Err(())
    } else {
        Ok(Some(ranges))
    }
}

fn build_file_response(
    status: StatusCode,
    content_length: u64,
    content_range: Option<String>,
    content_type: Option<String>,
    body: Body,
) -> Response {
    let mut builder = Response::builder()
        .status(status)
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::CACHE_CONTROL, "private, no-store")
        .header(header::CONTENT_LENGTH, content_length.to_string())
        .header(
            header::CONTENT_TYPE,
            content_type.unwrap_or_else(|| "application/octet-stream".into()),
        );
    if let Some(value) = content_range {
        builder = builder.header(header::CONTENT_RANGE, value);
    }
    builder
        .body(body)
        .unwrap_or_else(|_| empty_file_response(StatusCode::INTERNAL_SERVER_ERROR))
}

fn range_not_satisfiable(file_length: u64) -> Response {
    let mut response = empty_file_response(StatusCode::RANGE_NOT_SATISFIABLE);
    response
        .headers_mut()
        .insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    if let Ok(value) = HeaderValue::from_str(&format!("bytes */{file_length}")) {
        response.headers_mut().insert(header::CONTENT_RANGE, value);
    }
    response
}

fn empty_file_response(status: StatusCode) -> Response {
    Response::builder()
        .status(status)
        .header(header::CACHE_CONTROL, "private, no-store")
        .header(header::CONTENT_LENGTH, "0")
        .body(Body::empty())
        .unwrap_or_else(|_| Response::new(Body::empty()))
}

async fn live() -> impl IntoResponse {
    (StatusCode::OK, [(header::CACHE_CONTROL, "no-store")], "ok")
}

async fn ready(State(state): State<GatewayState>) -> Response {
    if state.ready.load(Ordering::Acquire) {
        let body = serde_json::json!({
            "status": "ready",
            "snapshot_generation": state.generation().to_string(),
            "telemetry": {
                "queued": state.telemetry_metrics.queued.load(Ordering::Relaxed).to_string(),
                "dropped": state.telemetry_metrics.dropped.load(Ordering::Relaxed).to_string(),
                "delivered": state.telemetry_metrics.delivered.load(Ordering::Relaxed).to_string(),
                "delivery_failed": state.telemetry_metrics.delivery_failed.load(Ordering::Relaxed).to_string(),
            },
        });
        (
            StatusCode::OK,
            [(header::CACHE_CONTROL, "no-store")],
            axum::Json(body),
        )
            .into_response()
    } else {
        telegram_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "Route snapshot unavailable",
        )
    }
}

async fn drain_route(State(state): State<GatewayState>, request: Request) -> Response {
    if !valid_internal_authorization(request.headers(), &state.sync_token_digest) {
        let mut response = admin_error(StatusCode::UNAUTHORIZED, "unauthorized");
        response
            .headers_mut()
            .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
        return response;
    }
    let bytes = match axum::body::to_bytes(request.into_body(), MAX_DRAIN_REQUEST_BYTES).await {
        Ok(bytes) => bytes,
        Err(_) => return admin_error(StatusCode::PAYLOAD_TOO_LARGE, "request_too_large"),
    };
    let drain: DrainRequest = match serde_json::from_slice(&bytes) {
        Ok(drain) => drain,
        Err(_) => return admin_error(StatusCode::BAD_REQUEST, "invalid_request"),
    };
    if drain.schema_version != SNAPSHOT_SCHEMA_VERSION
        || drain.minimum_snapshot_generation == 0
        || !valid_token_lookup_hash(&drain.token_lookup_hash)
    {
        return admin_error(StatusCode::BAD_REQUEST, "invalid_request");
    }
    let Some(telegram_bot_id) = telegram_bot_id(&drain.bot_token) else {
        return admin_error(StatusCode::BAD_REQUEST, "invalid_request");
    };
    if bot_public_id(
        &state.public_id_key,
        drain.bot_token.as_bytes(),
        drain.telegram_test_dc,
    ) != drain.token_lookup_hash
    {
        return admin_error(StatusCode::BAD_REQUEST, "invalid_request");
    }
    let observed = match state
        .drain_observation(&drain.token_lookup_hash, drain.minimum_snapshot_generation)
    {
        Ok(observed) => observed,
        Err(_) => return admin_error(StatusCode::SERVICE_UNAVAILABLE, "state_unavailable"),
    };
    let official = if observed.snapshot_generation >= drain.minimum_snapshot_generation
        && !observed.route_present
    {
        let command = OfficialDrainCommand {
            schema_version: SNAPSHOT_SCHEMA_VERSION,
            bot_token: &drain.bot_token,
            telegram_test_dc: drain.telegram_test_dc,
            route_generation: observed.snapshot_generation.to_string(),
        };
        let standard = request_official_drain(
            &state.client,
            &state.standard_control_url,
            &state.sync_token,
            &command,
            telegram_bot_id,
        );
        let local = request_official_drain(
            &state.client,
            &state.local_control_url,
            &state.sync_token,
            &command,
            telegram_bot_id,
        );
        let (standard, local) = tokio::join!(standard, local);
        OfficialDrainResult { standard, local }
    } else {
        OfficialDrainResult {
            standard: None,
            local: None,
        }
    };
    let official_fenced = official.fences_armed();
    let drained = observed.drained && official_fenced && official.is_idle();
    (
        StatusCode::OK,
        [(header::CACHE_CONTROL, "no-store")],
        axum::Json(DrainResponse {
            schema_version: SNAPSHOT_SCHEMA_VERSION,
            drained,
            snapshot_generation: observed.snapshot_generation.to_string(),
            route_present: observed.route_present,
            in_flight: observed.in_flight.to_string(),
            official_fenced,
            official_active_requests: official.response_counts(),
        }),
    )
        .into_response()
}

async fn request_official_drain(
    client: &Client,
    url: &Url,
    sync_token: &str,
    command: &OfficialDrainCommand<'_>,
    expected_bot_id: u64,
) -> Option<u64> {
    let response = client
        .post(url.clone())
        .bearer_auth(sync_token)
        .json(command)
        .timeout(Duration::from_secs(2))
        .send()
        .await
        .ok()?;
    if response.status() != StatusCode::OK
        || response
            .content_length()
            .is_some_and(|length| length > MAX_OFFICIAL_DRAIN_RESPONSE_BYTES as u64)
    {
        return None;
    }
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.ok()?;
        if body
            .len()
            .checked_add(chunk.len())
            .is_none_or(|length| length > MAX_OFFICIAL_DRAIN_RESPONSE_BYTES)
        {
            return None;
        }
        body.extend_from_slice(&chunk);
    }
    let proof: OfficialDrainProof = serde_json::from_slice(&body).ok()?;
    if proof.schema_version != SNAPSHOT_SCHEMA_VERSION
        || !proof.fenced
        || proof.telegram_bot_id.parse::<u64>().ok() != Some(expected_bot_id)
        || proof.telegram_test_dc != command.telegram_test_dc
        || proof.route_generation != command.route_generation
    {
        return None;
    }
    proof.active_requests.parse().ok()
}

fn admin_error(status: StatusCode, code: &'static str) -> Response {
    (
        status,
        [(header::CACHE_CONTROL, "no-store")],
        axum::Json(serde_json::json!({
            "error": { "code": code },
        })),
    )
        .into_response()
}

fn copy_end_to_end_headers(source: &HeaderMap, target: &mut HeaderMap) {
    let connection_headers = connection_nominated_headers(source);
    for (name, value) in source {
        if !hop_by_hop(name) && !connection_headers.contains(name) {
            target.append(name, value.clone());
        }
    }
}

fn connection_nominated_headers(headers: &HeaderMap) -> HashSet<HeaderName> {
    headers
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .filter_map(|name| HeaderName::from_bytes(name.trim().as_bytes()).ok())
        .collect()
}

fn hop_by_hop(name: &HeaderName) -> bool {
    matches!(
        name.as_str().to_ascii_lowercase().as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "proxy-connection"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
    )
}

fn telegram_error(status: StatusCode, description: &'static str) -> Response {
    let body = serde_json::json!({
        "ok": false,
        "error_code": status.as_u16(),
        "description": description,
    });
    (
        status,
        [(header::CACHE_CONTROL, "no-store")],
        axum::Json(body),
    )
        .into_response()
}

fn telemetry_event(
    token_lookup_hash: String,
    pool: Pool,
    method: String,
    upstream_status: StatusCode,
    latency: Duration,
) -> TelemetryEvent {
    TelemetryEvent::ApiCall(ApiCallTelemetryEvent {
        schema_version: 1,
        token_lookup_hash,
        pool,
        method,
        upstream_status: upstream_status.as_u16(),
        latency_ms: latency.as_millis().min(u64::MAX as u128) as u64,
        observed_at_unix_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(u64::MAX as u128) as u64,
        _queue_permit: None,
    })
}

fn outbound_telemetry_event(
    observation: &OutboundObservation,
    observed_at_unix_us: u64,
    message: ObservedOutboundMessage,
) -> TelemetryEvent {
    let mut event = TelemetryEvent::OutboundMessage(Box::new(OutboundMessageTelemetryEvent {
        schema_version: 1,
        kind: "outbound_message",
        token_lookup_hash: observation.token_lookup_hash.clone(),
        pool: observation.pool,
        method: observation.method.clone(),
        upstream_status: observation.upstream_status,
        observed_at_unix_us,
        message,
        _queue_permit: None,
    }));
    // A rich payload is optional observation data. If duplicating text and
    // metadata would make a single event exceed the bounded delivery batch,
    // keep the compact message identity instead of growing the queue.
    if telemetry_event_json_len(&event) > MAX_TELEMETRY_BATCH_BYTES.saturating_sub(64)
        && let TelemetryEvent::OutboundMessage(event) = &mut event
    {
        event.message.payload = None;
    }
    event
}

fn outbound_messages_from_response(body: &[u8]) -> Vec<ObservedOutboundMessage> {
    let Ok(envelope) = serde_json::from_slice::<serde_json::Value>(body) else {
        return Vec::new();
    };
    if envelope.get("ok").and_then(serde_json::Value::as_bool) != Some(true) {
        return Vec::new();
    }
    let Some(result) = envelope.get("result") else {
        return Vec::new();
    };
    match result {
        serde_json::Value::Object(_) => observed_outbound_message(result).into_iter().collect(),
        serde_json::Value::Array(messages) => messages
            .iter()
            .take(100)
            .filter_map(observed_outbound_message)
            .collect(),
        _ => Vec::new(),
    }
}

fn observed_outbound_message(value: &serde_json::Value) -> Option<ObservedOutboundMessage> {
    let telegram_message_id = value.get("message_id").and_then(serde_json::Value::as_i64);
    if telegram_message_id.is_some_and(|message_id| message_id < 0) {
        return None;
    }
    let receiver_user_id = value
        .pointer("/receiver_user/id")
        .or_else(|| value.pointer("/receiver/id"))
        .and_then(serde_json::Value::as_i64);
    let ephemeral_message_id = value
        .get("ephemeral_message_id")
        .and_then(serde_json::Value::as_i64);
    let chat = value.get("chat").and_then(serde_json::Value::as_object);
    let chat_id = chat
        .and_then(|chat| chat.get("id"))
        .and_then(serde_json::Value::as_i64)
        .filter(|chat_id| *chat_id != 0)
        .or(receiver_user_id)?;
    let durable_message = telegram_message_id.is_some_and(|message_id| message_id > 0);
    let ephemeral_message = receiver_user_id.is_some() && ephemeral_message_id.is_some();
    if !durable_message && !ephemeral_message {
        return None;
    }
    let first_name = chat
        .and_then(|chat| chat.get("first_name"))
        .and_then(serde_json::Value::as_str);
    let last_name = chat
        .and_then(|chat| chat.get("last_name"))
        .and_then(serde_json::Value::as_str);
    let display_name = match (first_name, last_name) {
        (Some(first), Some(last)) => Some(format!("{first} {last}")),
        (Some(first), None) => Some(first.to_owned()),
        (None, Some(last)) => Some(last.to_owned()),
        (None, None) => None,
    };
    Some(ObservedOutboundMessage {
        chat_id,
        telegram_message_id,
        receiver_user_id,
        ephemeral_message_id,
        business_connection_id: value
            .get("business_connection_id")
            .and_then(serde_json::Value::as_str)
            .and_then(|value| bounded_telemetry_string(value, 512, 2 * 1024)),
        guest_query_id: value
            .get("guest_query_id")
            .and_then(serde_json::Value::as_str)
            .and_then(|value| bounded_telemetry_string(value, 512, 2 * 1024)),
        message_thread_id: value
            .get("message_thread_id")
            .and_then(serde_json::Value::as_i64),
        direct_messages_topic_id: value
            .pointer("/direct_messages_topic/topic_id")
            .or_else(|| value.pointer("/direct_messages_topic/id"))
            .and_then(serde_json::Value::as_i64),
        text: value
            .get("text")
            .or_else(|| value.get("caption"))
            .and_then(serde_json::Value::as_str)
            .and_then(|value| bounded_telemetry_string(value, 4_096, 16 * 1024)),
        chat_type: chat
            .and_then(|chat| chat.get("type"))
            .and_then(serde_json::Value::as_str)
            .and_then(|value| bounded_telemetry_string(value, 64, 256)),
        title: chat
            .and_then(|chat| chat.get("title"))
            .and_then(serde_json::Value::as_str)
            .and_then(|value| bounded_telemetry_string(value, 512, 2 * 1024)),
        username: chat
            .and_then(|chat| chat.get("username"))
            .and_then(serde_json::Value::as_str)
            .and_then(|value| bounded_telemetry_string(value, 128, 512)),
        display_name: display_name
            .as_deref()
            .and_then(|value| bounded_telemetry_string(value, 512, 2 * 1024)),
        payload: sanitized_outbound_payload(value).map(Box::new),
    })
}

fn sanitized_outbound_payload(value: &serde_json::Value) -> Option<serde_json::Value> {
    let payload = sanitize_outbound_value(value, 0)?;
    (serde_json::to_vec(&payload).ok()?.len() <= MAX_OUTBOUND_PAYLOAD_BYTES).then_some(payload)
}

fn sanitize_outbound_value(value: &serde_json::Value, depth: usize) -> Option<serde_json::Value> {
    if depth > MAX_OUTBOUND_PAYLOAD_DEPTH {
        return None;
    }
    match value {
        serde_json::Value::Object(values) => {
            let mut sanitized = serde_json::Map::new();
            for (key, value) in values.iter().take(MAX_OUTBOUND_OBJECT_FIELDS) {
                let normalized = key.to_ascii_lowercase();
                if normalized == "file_path"
                    || normalized == "authorization"
                    || normalized.contains("token")
                {
                    continue;
                }
                if let Some(value) = sanitize_outbound_value(value, depth + 1) {
                    sanitized.insert(key.clone(), value);
                }
            }
            Some(serde_json::Value::Object(sanitized))
        }
        serde_json::Value::Array(values) => Some(serde_json::Value::Array(
            values
                .iter()
                .take(MAX_OUTBOUND_ARRAY_ITEMS)
                .filter_map(|value| sanitize_outbound_value(value, depth + 1))
                .collect(),
        )),
        // Bot API 10.2 RichText permits 32,768 Unicode characters. Keep the
        // complete legal scalar (including four-byte UTF-8) while the enclosing
        // sanitized payload and queue retain their independent byte budgets.
        serde_json::Value::String(value) => {
            bounded_telemetry_string(value, 32_768, 32_768 * char::MAX_LEN_UTF8)
                .map(serde_json::Value::String)
        }
        value => Some(value.clone()),
    }
}

fn bounded_telemetry_string(value: &str, max_chars: usize, max_bytes: usize) -> Option<String> {
    if value.contains('\0') {
        return None;
    }
    let bounded = value.chars().take(max_chars).collect::<String>();
    if bounded.len() <= max_bytes {
        Some(bounded)
    } else {
        let mut end = max_bytes;
        while !bounded.is_char_boundary(end) {
            end -= 1;
        }
        Some(bounded[..end].to_owned())
    }
}

fn telegram_method(path: ParsedPublicPath<'_>) -> String {
    let PublicPathKind::Api { method } = path.kind else {
        return "downloadFile".into();
    };
    if !method.is_empty()
        && method.len() <= 64
        && method
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
    {
        String::from_utf8_lossy(method).into_owned()
    } else {
        "invalid".into()
    }
}

fn outbound_response_candidate(method: &str) -> bool {
    let method = method.to_ascii_lowercase();
    method.starts_with("send")
        || method.starts_with("forward")
        || method.starts_with("editmessage")
        || method.starts_with("editephemeralmessage")
        || method == "answerguestquery"
        || method == "stopmessagelivelocation"
        || method == "setgamescore"
}

fn parse_public_request_path(path: &[u8]) -> Option<ParsedPublicPath<'_>> {
    parse_decoded_file_request_path(path).or_else(|| parse_decoded_api_request_path(path))
}

fn parse_decoded_api_request_path(path: &[u8]) -> Option<ParsedPublicPath<'_>> {
    let suffix = path.strip_prefix(b"/bot")?;
    let token_end = suffix.iter().position(|byte| *byte == b'/')?;
    let token = &suffix[..token_end];
    let mut remainder = &suffix[token_end..];
    let test_dc = remainder.starts_with(b"/test");
    if test_dc {
        remainder = &remainder[b"/test".len()..];
    }
    let method = remainder.strip_prefix(b"/")?;
    Some(ParsedPublicPath {
        token,
        test_dc,
        kind: PublicPathKind::Api { method },
    })
}

fn parse_decoded_file_request_path(path: &[u8]) -> Option<ParsedPublicPath<'_>> {
    let suffix = path.strip_prefix(b"/file/bot")?;
    let token_end = suffix.iter().position(|byte| *byte == b'/')?;
    let token = &suffix[..token_end];
    let remainder = &suffix[token_end + 1..];
    if token.is_empty() || remainder.is_empty() {
        return None;
    }
    let (test_dc, file_path) = remainder
        .strip_prefix(b"test/")
        .map_or((false, remainder), |path| (true, path));
    if file_path.is_empty() {
        return None;
    }
    Some(ParsedPublicPath {
        token,
        test_dc,
        kind: PublicPathKind::File { file_path },
    })
}

impl ParsedPublicPath<'_> {
    fn into_owned_file_request(self) -> FileRequestPath {
        let PublicPathKind::File { file_path } = self.kind else {
            unreachable!("only file paths are converted to file requests");
        };
        FileRequestPath {
            token: self.token.to_vec(),
            test_dc: self.test_dc,
            file_path: file_path.to_vec(),
        }
    }
}

fn derive_public_id_key(value: &str) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"phenogram:public-id:v1");
    hash.update(b"\0");
    hash.update(value.as_bytes());
    hash.finalize().into()
}

fn bot_public_id(key: &[u8; 32], token: &[u8], test_dc: bool) -> String {
    let mut mac = <HmacSha256 as Mac>::new_from_slice(key).expect("HMAC accepts a 32-byte key");
    mac.update(if test_dc {
        b"telegram-dc:v1:test\0"
    } else {
        b"telegram-dc:v1:prod\0"
    });
    mac.update(token);
    let digest = mac.finalize().into_bytes();
    format!("phg_{}", URL_SAFE_NO_PAD.encode(&digest[..18]))
}

fn valid_token_lookup_hash(value: &str) -> bool {
    value.len() == 28
        && value.starts_with("phg_")
        && value[4..]
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn telegram_bot_id(token: &str) -> Option<u64> {
    if token.is_empty() || token.len() > 80 || token.starts_with('0') || token.contains('/') {
        return None;
    }
    let (id, secret) = token.split_once(':')?;
    if id.is_empty() || secret.is_empty() || !id.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let id = id.parse::<u64>().ok()?;
    (id > 0 && id < (1_u64 << 54)).then_some(id)
}

fn upstream(name: &str) -> Result<String, String> {
    let value = required(name)?;
    let parsed = Url::parse(&value).map_err(|error| format!("{name} is invalid: {error}"))?;
    if !matches!(parsed.scheme(), "http" | "https")
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err(format!(
            "{name} must be an HTTP(S) origin without credentials, query, or fragment"
        ));
    }
    Ok(value.trim_end_matches('/').to_string())
}

fn internal_url(name: &str) -> Result<Url, String> {
    let value = required(name)?;
    let parsed = Url::parse(&value).map_err(|error| format!("{name} is invalid: {error}"))?;
    if !matches!(parsed.scheme(), "http" | "https")
        || !parsed.username().is_empty()
        || parsed.password().is_some()
    {
        return Err(format!(
            "{name} must be an HTTP(S) URL without embedded credentials"
        ));
    }
    Ok(parsed)
}

fn required(name: &str) -> Result<String, String> {
    optional(name).ok_or_else(|| format!("{name} is required"))
}

fn optional(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.trim().is_empty())
}

fn parse_addr(name: &str, value: &str) -> Result<SocketAddr, String> {
    SocketAddr::from_str(value).map_err(|error| format!("{name} is invalid: {error}"))
}

fn parse_u64(name: &str, value: &str, minimum: u64, maximum: u64) -> Result<u64, String> {
    let parsed = value
        .parse::<u64>()
        .map_err(|error| format!("{name} is invalid: {error}"))?;
    if !(minimum..=maximum).contains(&parsed) {
        return Err(format!("{name} must be between {minimum} and {maximum}"));
    }
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use axum::{
        body::{Bytes, to_bytes},
        http::Method,
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        sync::{Semaphore, mpsc},
    };

    use super::*;

    #[derive(Debug)]
    struct CapturedRequest {
        method: Method,
        path_and_query: String,
        content_type: Option<String>,
        content_length: Option<String>,
        removed_header: Option<String>,
        body: Vec<u8>,
    }

    #[test]
    fn token_lookup_hash_matches_the_control_plane_algorithm() {
        let key = derive_public_id_key(&"b".repeat(32));
        assert_eq!(
            bot_public_id(&key, b"123:secret", false),
            "phg_8nXOV-QrC3mmm517ijpZlMjV"
        );
        assert_eq!(
            bot_public_id(&key, b"123:secret", true),
            "phg_kD1sFex4hdK1HJcOC4JG4E5k"
        );
    }

    #[test]
    fn extracts_successful_text_and_media_messages_from_telegram_results() {
        let text = serde_json::json!({
            "ok": true,
            "result": {
                "message_id": 41,
                "date": 1_786_620_000,
                "chat": {
                    "id": 99,
                    "type": "private",
                    "username": "ada",
                    "first_name": "Ada",
                    "last_name": "Lovelace"
                },
                "text": "hello"
            }
        });
        let messages = outbound_messages_from_response(text.to_string().as_bytes());
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].chat_id, 99);
        assert_eq!(messages[0].telegram_message_id, Some(41));
        assert_eq!(messages[0].text.as_deref(), Some("hello"));
        assert_eq!(messages[0].username.as_deref(), Some("ada"));
        assert_eq!(messages[0].display_name.as_deref(), Some("Ada Lovelace"));
        assert_eq!(messages[0].payload.as_ref().unwrap()["text"], "hello");

        let media_group = serde_json::json!({
            "ok": true,
            "result": [{
                "message_id": 42,
                "date": 1_786_620_001,
                "chat": {"id": -1007, "type": "supergroup", "title": "Launch"},
                "photo": [{"file_id": "photo", "file_unique_id": "unique", "width": 10, "height": 10}],
                "caption": "first photo"
            }, {
                "message_id": 43,
                "date": 1_786_620_001,
                "chat": {"id": -1007, "type": "supergroup", "title": "Launch"},
                "video": {"file_id": "video", "file_unique_id": "unique-video", "width": 10, "height": 10, "duration": 1}
            }]
        });
        let messages = outbound_messages_from_response(media_group.to_string().as_bytes());
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].text.as_deref(), Some("first photo"));
        assert_eq!(messages[1].text, None);
        assert_eq!(messages[1].title.as_deref(), Some("Launch"));
        assert_eq!(
            messages[0].payload.as_ref().unwrap()["photo"][0]["file_id"],
            "photo"
        );

        let ephemeral = serde_json::json!({
            "ok": true,
            "result": {
                "message_id": 0,
                "ephemeral_message_id": 73,
                "receiver_user": {"id": 555, "first_name": "Grace"},
                "text": "short-lived"
            }
        });
        let messages = outbound_messages_from_response(ephemeral.to_string().as_bytes());
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].chat_id, 555);
        assert_eq!(messages[0].telegram_message_id, Some(0));
        assert_eq!(messages[0].receiver_user_id, Some(555));
        assert_eq!(messages[0].ephemeral_message_id, Some(73));

        let secrets = serde_json::json!({
            "ok": true,
            "result": {
                "message_id": 44,
                "chat": {"id": 99, "type": "private"},
                "document": {
                    "file_id": "safe-file-id",
                    "file_path": "/native/private/path",
                    "provider_token": "must-not-leave-the-gateway"
                }
            }
        });
        let messages = outbound_messages_from_response(secrets.to_string().as_bytes());
        let payload = messages[0].payload.as_ref().unwrap();
        assert_eq!(payload["document"]["file_id"], "safe-file-id");
        assert!(payload["document"].get("file_path").is_none());
        assert!(payload["document"].get("provider_token").is_none());

        let maximum_rich_text = "🧪".repeat(32_768);
        let rich = serde_json::json!({
            "ok": true,
            "result": {
                "message_id": 45,
                "chat": {"id": 99, "type": "private"},
                "rich_message": {"text": maximum_rich_text}
            }
        });
        let messages = outbound_messages_from_response(rich.to_string().as_bytes());
        assert_eq!(
            messages[0].payload.as_ref().unwrap()["rich_message"]["text"]
                .as_str()
                .expect("sanitized rich text")
                .chars()
                .count(),
            32_768
        );

        assert!(
            outbound_messages_from_response(
                br#"{"ok":false,"result":{"message_id":1,"chat":{"id":1}}}"#
            )
            .is_empty()
        );
        assert!(
            outbound_messages_from_response(br#"{"ok":true,"result":{"message_id":1}}"#).is_empty()
        );
    }

    #[tokio::test]
    async fn outbound_observation_is_fail_open_and_requires_a_clean_bounded_eof() {
        let config = test_config(
            "http://127.0.0.1:1",
            "http://127.0.0.1/api/internal/data-plane/telemetry",
        );
        let (state, mut receiver) = GatewayState::new(&config).expect("gateway state");
        let oversized_event = outbound_telemetry_event(
            &test_outbound_observation(&state),
            1_786_620_000_000_000,
            ObservedOutboundMessage {
                chat_id: 99,
                telegram_message_id: Some(9000),
                receiver_user_id: None,
                ephemeral_message_id: None,
                business_connection_id: None,
                guest_query_id: None,
                message_thread_id: None,
                direct_messages_topic_id: None,
                text: Some("x".repeat(16 * 1024)),
                chat_type: Some("private".into()),
                title: Some("t".repeat(2 * 1024)),
                username: Some("u".repeat(512)),
                display_name: Some("d".repeat(2 * 1024)),
                payload: Some(Box::new(serde_json::json!({
                    "blocks": vec!["p".repeat(100_000); 6]
                }))),
            },
        );
        assert!(telemetry_event_json_len(&oversized_event) <= MAX_TELEMETRY_BATCH_BYTES);
        let TelemetryEvent::OutboundMessage(oversized_event) = oversized_event else {
            panic!("expected outbound telemetry");
        };
        assert!(oversized_event.message.payload.is_none());

        let response = serde_json::json!({
            "ok": true,
            "result": {
                "message_id": 9001,
                "date": 1_786_620_000,
                "chat": {"id": 99, "type": "private"},
                "text": "observed"
            }
        })
        .to_string();
        let mut stream = OutboundObservationStream::new(
            futures_util::stream::iter([Ok(Bytes::from(response.clone()))]),
            state.clone(),
            test_outbound_observation(&state),
            None,
        );
        let mut delivered = Vec::new();
        while let Some(chunk) = stream.next().await {
            delivered.extend_from_slice(&chunk.expect("unchanged response chunk"));
        }
        assert_eq!(delivered, response.as_bytes());
        let event = tokio::time::timeout(Duration::from_secs(1), receiver.recv())
            .await
            .expect("outbound observation timeout")
            .expect("outbound observation");
        let TelemetryEvent::OutboundMessage(event) = event else {
            panic!("expected outbound telemetry");
        };
        assert_eq!(event.method, "sendMessage");
        assert_eq!(event.message.chat_id, 99);
        assert_eq!(event.message.telegram_message_id, Some(9001));
        assert_eq!(event.message.payload.as_ref().unwrap()["text"], "observed");
        let first_observed_at_unix_us = event.observed_at_unix_us;

        let later_response = response.replace("9001", "9002");
        let mut later = OutboundObservationStream::new(
            futures_util::stream::iter([Ok::<_, io::Error>(Bytes::from(later_response.clone()))]),
            state.clone(),
            test_outbound_observation(&state),
            Some(later_response.len()),
        );
        assert_eq!(
            later
                .next()
                .await
                .expect("declared response chunk")
                .unwrap(),
            later_response.as_bytes()
        );
        // A declared Content-Length can end downstream consumption here; the
        // timestamp and detached observation must already be committed.
        drop(later);
        let later_event = tokio::time::timeout(Duration::from_secs(1), receiver.recv())
            .await
            .expect("later outbound observation timeout")
            .expect("later outbound observation");
        let TelemetryEvent::OutboundMessage(later_event) = later_event else {
            panic!("expected later outbound telemetry");
        };
        assert_eq!(later_event.message.telegram_message_id, Some(9002));
        assert!(later_event.observed_at_unix_us > first_observed_at_unix_us);

        let mut oversized = OutboundObservationStream::new(
            futures_util::stream::iter([Ok(Bytes::from(vec![
                b'x';
                MAX_OUTBOUND_RESPONSE_BYTES + 1
            ]))]),
            state.clone(),
            test_outbound_observation(&state),
            None,
        );
        while oversized.next().await.is_some() {}
        assert!(
            tokio::time::timeout(Duration::from_millis(20), receiver.recv())
                .await
                .is_err()
        );

        let mut failed = OutboundObservationStream::new(
            futures_util::stream::iter([
                Ok(Bytes::from(response.clone())),
                Err(io::Error::other("simulated stream failure")),
            ]),
            state.clone(),
            test_outbound_observation(&state),
            None,
        );
        assert!(failed.next().await.expect("first chunk").is_ok());
        assert!(failed.next().await.expect("stream error").is_err());
        drop(failed);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), receiver.recv())
                .await
                .is_err()
        );

        let mut disconnected = OutboundObservationStream::new(
            futures_util::stream::iter([
                Ok::<_, io::Error>(Bytes::from(response.clone())),
                Ok(Bytes::from_static(b"not consumed")),
            ]),
            state.clone(),
            test_outbound_observation(&state),
            None,
        );
        assert!(disconnected.next().await.expect("first chunk").is_ok());
        drop(disconnected);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), receiver.recv())
                .await
                .is_err()
        );

        let mut full_config = config;
        full_config.telemetry_queue_capacity = 1;
        let (full_state, mut full_receiver) =
            GatewayState::new(&full_config).expect("full gateway state");
        full_state.record_telemetry(telemetry_event(
            "phg_012345678901234567890123".into(),
            Pool::Standard,
            "getMe".into(),
            StatusCode::OK,
            Duration::ZERO,
        ));
        let mut queue_full = OutboundObservationStream::new(
            futures_util::stream::iter([Ok::<_, io::Error>(Bytes::from(response.clone()))]),
            full_state.clone(),
            test_outbound_observation(&full_state),
            None,
        );
        let mut queue_full_response = Vec::new();
        while let Some(chunk) = queue_full.next().await {
            queue_full_response.extend_from_slice(&chunk.expect("queue-full response chunk"));
        }
        assert_eq!(queue_full_response, response.as_bytes());
        tokio::time::timeout(Duration::from_secs(1), async {
            while full_state.telemetry_metrics.dropped.load(Ordering::Relaxed) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("queue-full observation drop");
        assert!(matches!(
            full_receiver.recv().await,
            Some(TelemetryEvent::ApiCall(_))
        ));
        assert!(full_receiver.try_recv().is_err());
    }

    #[tokio::test]
    async fn saturated_outbound_budget_streams_unchanged_and_releases_after_enqueue() {
        let telegram_body = serde_json::json!({
            "ok": true,
            "result": {
                "message_id": 9100,
                "date": 1_786_620_000,
                "chat": {"id": 99, "type": "private"},
                "text": "budgeted"
            }
        })
        .to_string();
        let upstream_body = telegram_body.clone();
        let upstream = Router::new().fallback(any(move || {
            let upstream_body = upstream_body.clone();
            async move { (StatusCode::OK, upstream_body) }
        }));
        let upstream_url = spawn(upstream).await;
        let config = test_config(
            &upstream_url,
            "http://127.0.0.1/api/internal/data-plane/telemetry",
        );
        let (state, mut receiver) = GatewayState::new(&config).expect("gateway state");
        let token_lookup_hash = bot_public_id(&state.public_id_key, b"123:secret", false);
        state
            .install(RouteSnapshot {
                schema_version: 1,
                generation: 1,
                routes: vec![RouteRecord {
                    token_lookup_hash,
                    pool: Pool::Standard,
                }],
            })
            .expect("install route");
        let gateway_url = spawn_public_http1(public_router(state.clone())).await;
        let mut held = (0..MAX_CONCURRENT_OUTBOUND_OBSERVATIONS)
            .map(|_| test_outbound_observation(&state))
            .collect::<Vec<_>>();
        assert_eq!(state.outbound_observation_slots.available_permits(), 0);

        let response = Client::new()
            .post(format!("{gateway_url}/bot123:secret/sendMessage"))
            .send()
            .await
            .expect("saturated response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.text().await.expect("saturated response body"),
            telegram_body
        );
        assert!(matches!(
            receiver.recv().await,
            Some(TelemetryEvent::ApiCall(_))
        ));
        assert!(
            tokio::time::timeout(Duration::from_millis(20), receiver.recv())
                .await
                .is_err()
        );
        assert_eq!(state.telemetry_metrics.dropped.load(Ordering::Relaxed), 1);

        drop(held.pop());
        let response = Client::new()
            .post(format!("{gateway_url}/bot123:secret/sendMessage"))
            .send()
            .await
            .expect("released-budget response");
        assert_eq!(
            response.text().await.expect("released-budget body"),
            telegram_body
        );
        assert!(matches!(
            receiver.recv().await,
            Some(TelemetryEvent::ApiCall(_))
        ));
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), receiver.recv())
                .await
                .expect("captured outbound timeout"),
            Some(TelemetryEvent::OutboundMessage(_))
        ));
        tokio::time::timeout(Duration::from_secs(1), async {
            while state.outbound_observation_slots.available_permits() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("observation budget release");
        drop(held);
        assert_eq!(
            state.outbound_observation_slots.available_permits(),
            MAX_CONCURRENT_OUTBOUND_OBSERVATIONS
        );
    }

    #[tokio::test]
    async fn trusted_bot_view_marker_is_stripped_and_suppresses_duplicate_observation() {
        let (capture_tx, mut capture_rx) = mpsc::channel(2);
        let upstream = Router::new()
            .fallback(any(
                |State(capture_tx): State<mpsc::Sender<bool>>, request: Request| async move {
                    capture_tx
                        .send(request.headers().contains_key(OBSERVATION_BYPASS_HEADER))
                        .await
                        .expect("capture receiver");
                    axum::Json(serde_json::json!({
                        "ok": true,
                        "result": {
                            "message_id": 9101,
                            "date": 1_786_620_000,
                            "chat": {"id": 99, "type": "private"},
                            "text": "operator reply"
                        }
                    }))
                },
            ))
            .with_state(capture_tx);
        let upstream_url = spawn(upstream).await;
        let config = test_config(
            &upstream_url,
            "http://127.0.0.1/api/internal/data-plane/telemetry",
        );
        let (state, mut receiver) = GatewayState::new(&config).expect("gateway state");
        state
            .install(RouteSnapshot {
                schema_version: 1,
                generation: 1,
                routes: vec![RouteRecord {
                    token_lookup_hash: bot_public_id(&state.public_id_key, b"123:secret", false),
                    pool: Pool::Standard,
                }],
            })
            .expect("install route");
        let gateway_url = spawn_public_http1(public_router(state)).await;
        let client = Client::new();

        let response = client
            .post(format!("{gateway_url}/bot123:secret/sendMessage"))
            .header(OBSERVATION_BYPASS_HEADER, "s".repeat(32))
            .send()
            .await
            .expect("trusted Bot View response");
        assert_eq!(response.status(), StatusCode::OK);
        assert!(!capture_rx.recv().await.expect("captured trusted request"));
        assert!(matches!(
            receiver.recv().await,
            Some(TelemetryEvent::ApiCall(_))
        ));
        assert!(
            tokio::time::timeout(Duration::from_millis(30), receiver.recv())
                .await
                .is_err(),
            "trusted Bot View request must not create a second outbound timeline row"
        );

        let response = client
            .post(format!("{gateway_url}/bot123:secret/sendMessage"))
            .header(OBSERVATION_BYPASS_HEADER, "not-the-sync-token")
            .send()
            .await
            .expect("untrusted marked response");
        assert_eq!(response.status(), StatusCode::OK);
        assert!(!capture_rx.recv().await.expect("captured untrusted request"));
        assert!(matches!(
            receiver.recv().await,
            Some(TelemetryEvent::ApiCall(_))
        ));
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), receiver.recv())
                .await
                .expect("untrusted observation timeout"),
            Some(TelemetryEvent::OutboundMessage(_))
        ));
    }

    #[test]
    fn recognizes_both_official_api_path_shapes() {
        let api = parse_public_request_path(b"/bot123:secret/SendMessage").expect("API path");
        assert_eq!(api.token, b"123:secret");
        assert!(!api.test_dc);
        assert_eq!(telegram_method(api), "SendMessage");
        let test = parse_public_request_path(b"/bot123:secret/test/getMe").expect("test path");
        assert_eq!(test.token, b"123:secret");
        assert!(test.test_dc);
        assert_eq!(telegram_method(test), "getMe");
        let file =
            parse_public_request_path(b"/file/bot123:secret/photos/a.jpg").expect("file path");
        assert_eq!(file.token, b"123:secret");
        assert_eq!(telegram_method(file), "downloadFile");
        assert!(parse_public_request_path(b"/events").is_none());
        let empty = parse_public_request_path(b"/bot/getMe").expect("empty token path");
        assert_eq!(empty.token, b"");
        let invalid = parse_public_request_path(b"/bot123:secret/not/a/method")
            .expect("structurally valid path");
        assert_eq!(telegram_method(invalid), "invalid");
    }

    #[test]
    fn validates_official_path_structure_before_token_authentication() {
        for path in [
            "/bot123:secret/getMe",
            "/bot123:secret/",
            "/bot123:secret/test/getMe",
            "/bot123:secret/test/",
            "/bot/getMe",
            "/file/bot123:secret/documents/file.bin",
            "/file/bot123:secret/test/documents/file.bin",
        ] {
            assert!(
                parse_public_request_path(path.as_bytes()).is_some(),
                "valid path: {path}"
            );
        }
        for path in [
            "/bot123:secret",
            "/bot123:secret/test",
            "/bot456:other",
            "/bot456:other/test",
            "/file/bot123:secret",
            "/file/bot123:secret/",
            "/file/bot123:secret/test/",
            "/file/bot456:other",
            "/file/bot456:other/",
            "/events",
        ] {
            assert!(
                parse_public_request_path(path.as_bytes()).is_none(),
                "invalid path: {path}"
            );
        }
    }

    #[test]
    fn parses_standard_local_and_test_file_paths_without_changing_the_token() {
        let standard = parse_file_request_path("/file/bot123:secret/documents/file.bin")
            .expect("standard file path");
        assert_eq!(standard.token, b"123:secret");
        assert!(!standard.test_dc);
        assert_eq!(standard.file_path, b"documents/file.bin");

        let local = parse_file_request_path(
            "/file/bot123:secret//var/lib/telegram-bot-api/b-opaque/file.bin",
        )
        .expect("local file path");
        assert_eq!(
            local.file_path,
            b"/var/lib/telegram-bot-api/b-opaque/file.bin"
        );

        let test = parse_file_request_path("/file/bot123:secret/test/documents/file.bin")
            .expect("test file path");
        assert_eq!(test.token, b"123:secret");
        assert!(test.test_dc);
        assert_eq!(test.file_path, b"documents/file.bin");
        assert!(valid_file_server_root(Path::new(
            "/var/lib/telegram-bot-api"
        )));
        assert!(!valid_file_server_root(Path::new("/")));
        assert!(!valid_file_server_root(Path::new("/var/../etc")));
        assert!(!valid_file_server_root(Path::new("relative")));
    }

    #[test]
    fn parses_closed_open_suffix_and_multiple_byte_ranges() {
        assert_eq!(
            parse_byte_ranges(&HeaderValue::from_static("bytes=2-5"), 10),
            Ok(Some(vec![ByteRange { start: 2, end: 5 }]))
        );
        assert_eq!(
            parse_byte_ranges(&HeaderValue::from_static("bytes=7-"), 10),
            Ok(Some(vec![ByteRange { start: 7, end: 9 }]))
        );
        assert_eq!(
            parse_byte_ranges(&HeaderValue::from_static("bytes=-3"), 10),
            Ok(Some(vec![ByteRange { start: 7, end: 9 }]))
        );
        assert_eq!(
            parse_byte_ranges(&HeaderValue::from_static("bytes=0-0,9-20"), 10),
            Ok(Some(vec![
                ByteRange { start: 0, end: 0 },
                ByteRange { start: 9, end: 9 }
            ]))
        );
        assert!(parse_byte_ranges(&HeaderValue::from_static("bytes=20-"), 10).is_err());
        assert_eq!(
            parse_byte_ranges(&HeaderValue::from_static("items=0-1"), 10),
            Ok(None)
        );
    }

    #[test]
    fn native_storage_directories_match_the_pinned_official_server() {
        assert_eq!(
            native_bot_directories(b"123:secret", false).expect("production directories"),
            vec![PathBuf::from("123:secret"), PathBuf::from("123~secret")]
        );
        assert_eq!(
            native_bot_directories(b"123:secret", true).expect("test directories"),
            vec![PathBuf::from("123:secret:T"), PathBuf::from("123~secret~T")]
        );
        assert!(native_bot_directories(b"", false).is_err());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn opens_native_colon_fallback_and_test_dc_directories() {
        let root = temporary_directory("native-directory-mapping");
        let fallback = root.join("777~fallback").join("documents");
        let test_dc = root.join("888:test:T").join("documents");
        tokio::fs::create_dir_all(&fallback)
            .await
            .expect("create fallback directory");
        tokio::fs::create_dir_all(&test_dc)
            .await
            .expect("create test-DC directory");
        tokio::fs::write(fallback.join("fallback.bin"), b"fallback")
            .await
            .expect("write fallback file");
        tokio::fs::write(test_dc.join("test.bin"), b"test-dc")
            .await
            .expect("write test-DC file");

        let state = test_file_server_state(root.clone(), Pool::Standard);
        let fallback_file = open_requested_file(
            &state,
            FileRequestPath {
                token: b"777:fallback".to_vec(),
                test_dc: false,
                file_path: b"documents/fallback.bin".to_vec(),
            },
        )
        .await
        .expect("open colon-fallback file");
        assert_eq!(fallback_file.length, 8);
        let test_file = open_requested_file(
            &state,
            FileRequestPath {
                token: b"888:test".to_vec(),
                test_dc: true,
                file_path: b"documents/test.bin".to_vec(),
            },
        )
        .await
        .expect("open test-DC file");
        assert_eq!(test_file.length, 7);
        drop((fallback_file, test_file));

        tokio::fs::remove_dir_all(root)
            .await
            .expect("remove native-directory test root");
    }

    // APFS rejects arbitrary non-UTF-8 names, while the production Linux
    // filesystem and official server preserve raw filename bytes.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn file_server_preserves_percent_decoded_non_utf8_filename_bytes() {
        use std::{ffi::OsString, os::unix::ffi::OsStringExt};

        let root = temporary_directory("non-utf8-file");
        let documents = root
            .join(native_directory("123:secret", false))
            .join("documents");
        tokio::fs::create_dir_all(&documents)
            .await
            .expect("create non-UTF8 file directory");
        let filename = OsString::from_vec(vec![b'f', 0xff, b'.', b'b', b'i', b'n']);
        tokio::fs::write(documents.join(filename), b"non-utf8")
            .await
            .expect("write non-UTF8 filename");

        let file_server_url = spawn(file_server_router(test_file_server_state(
            root.clone(),
            Pool::Standard,
        )))
        .await;
        let response = Client::new()
            .get(format!(
                "{file_server_url}/file/bot123%3Asecret/documents/f%FF.bin"
            ))
            .bearer_auth("s".repeat(32))
            .send()
            .await
            .expect("non-UTF8 filename response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.bytes().await.expect("non-UTF8 file body"),
            &b"non-utf8"[..]
        );

        tokio::fs::remove_dir_all(root)
            .await
            .expect("remove non-UTF8 file root");
    }

    #[test]
    fn rejects_duplicate_or_malformed_snapshot_routes() {
        let valid = "phg_012345678901234567890123".to_string();
        let duplicate = RouteSnapshot {
            schema_version: 1,
            generation: 7,
            routes: vec![
                RouteRecord {
                    token_lookup_hash: valid.clone(),
                    pool: Pool::Standard,
                },
                RouteRecord {
                    token_lookup_hash: valid,
                    pool: Pool::Local,
                },
            ],
        };
        assert!(RouteTable::try_from(duplicate).is_err());
    }

    #[test]
    fn a_generation_cannot_be_reused_for_different_routes() {
        let current = RouteTable::try_from(RouteSnapshot {
            schema_version: 1,
            generation: 8,
            routes: vec![RouteRecord {
                token_lookup_hash: "phg_012345678901234567890123".into(),
                pool: Pool::Standard,
            }],
        })
        .expect("current routes");
        let changed = RouteTable::try_from(RouteSnapshot {
            schema_version: 1,
            generation: 8,
            routes: vec![RouteRecord {
                token_lookup_hash: "phg_012345678901234567890123".into(),
                pool: Pool::Local,
            }],
        })
        .expect("changed routes");
        assert!(validate_generation(&current, &changed).is_err());
    }

    #[tokio::test]
    async fn last_good_routes_survive_a_control_plane_outage_and_restart() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "phenogram-gateway-test-{}-{unique}",
            std::process::id()
        ));
        let path = directory.join("routes.json");
        let config = test_config(
            "http://127.0.0.1:1",
            "http://127.0.0.1:1/api/internal/data-plane/telemetry",
        );
        let (first, _receiver) = GatewayState::new(&config).expect("first gateway");
        let token_lookup_hash = bot_public_id(&first.public_id_key, b"123:secret", false);
        let snapshot = RouteSnapshot {
            schema_version: 1,
            generation: 11,
            routes: vec![RouteRecord {
                token_lookup_hash,
                pool: Pool::Local,
            }],
        };
        let bytes = serde_json::to_vec(&snapshot).expect("snapshot JSON");
        persist_snapshot(&path, &bytes)
            .await
            .expect("persist snapshot");

        let (restarted, _receiver) = GatewayState::new(&config).expect("restarted gateway");
        assert!(
            restarted
                .load_last_good_snapshot(&path)
                .await
                .expect("load snapshot")
        );
        assert_eq!(
            restarted.route(b"123:secret", false).expect("route"),
            Some((
                bot_public_id(&restarted.public_id_key, b"123:secret", false),
                Pool::Local
            ))
        );
        assert!(restarted.ready.load(Ordering::Acquire));

        tokio::fs::remove_dir_all(directory)
            .await
            .expect("remove test directory");
    }

    #[tokio::test]
    async fn withdrawn_route_drains_only_after_the_admitted_response_body_finishes() {
        type DrainUpstreamState = (
            mpsc::Sender<()>,
            Arc<Semaphore>,
            Arc<AtomicUsize>,
            Arc<AtomicUsize>,
        );
        let (first_chunk_tx, mut first_chunk_rx) = mpsc::channel(1);
        let release_body = Arc::new(Semaphore::new(0));
        let upstream_requests = Arc::new(AtomicUsize::new(0));
        let official_active_requests = Arc::new(AtomicUsize::new(0));
        let upstream = Router::new()
            .fallback(any(
                |State((
                    first_chunk_tx,
                    release_body,
                    upstream_requests,
                    official_active_requests,
                )): State<DrainUpstreamState>,
                 request: Request| async move {
                    assert_eq!(
                        request
                            .headers()
                            .get(ROUTE_GENERATION_HEADER)
                            .and_then(|value| value.to_str().ok()),
                        Some("1")
                    );
                    upstream_requests.fetch_add(1, Ordering::SeqCst);
                    official_active_requests.store(1, Ordering::SeqCst);
                    let stream = futures_util::stream::unfold(0_u8, move |step| {
                        let first_chunk_tx = first_chunk_tx.clone();
                        let release_body = release_body.clone();
                        let official_active_requests = official_active_requests.clone();
                        async move {
                            match step {
                                0 => {
                                    first_chunk_tx.send(()).await.expect("first-chunk observer");
                                    Some((Ok::<_, io::Error>(Bytes::from_static(b"first")), 1))
                                }
                                1 => {
                                    let permit = release_body
                                        .acquire()
                                        .await
                                        .expect("body release semaphore");
                                    permit.forget();
                                    official_active_requests.store(0, Ordering::SeqCst);
                                    Some((Ok::<_, io::Error>(Bytes::from_static(b"-last")), 2))
                                }
                                _ => None,
                            }
                        }
                    });
                    Response::new(Body::from_stream(stream))
                },
            ))
            .with_state((
                first_chunk_tx,
                release_body.clone(),
                upstream_requests.clone(),
                official_active_requests.clone(),
            ));
        let upstream_url = spawn(upstream).await;
        let control = Router::new()
            .route(
                "/internal/official/drain",
                post(
                    |State(active): State<Arc<AtomicUsize>>, request: Request| async move {
                        let expected_authorization = format!("Bearer {}", "s".repeat(32));
                        assert_eq!(
                            request
                                .headers()
                                .get(header::AUTHORIZATION)
                                .and_then(|value| value.to_str().ok()),
                            Some(expected_authorization.as_str())
                        );
                        let command: serde_json::Value = serde_json::from_slice(
                            &to_bytes(request.into_body(), MAX_DRAIN_REQUEST_BYTES)
                                .await
                                .expect("control request body"),
                        )
                        .expect("control request JSON");
                        axum::Json(serde_json::json!({
                            "schema_version": 1,
                            "fenced": true,
                            "telegram_bot_id": "123",
                            "telegram_test_dc": false,
                            "route_generation": command["route_generation"],
                            "active_requests": active.load(Ordering::SeqCst).to_string(),
                        }))
                    },
                ),
            )
            .with_state(official_active_requests.clone());
        let control_url = spawn(control).await;
        let mut config = test_config(
            &upstream_url,
            "http://127.0.0.1/api/internal/data-plane/telemetry",
        );
        config.standard_control_url = Url::parse(&format!("{control_url}/internal/official/drain"))
            .expect("standard control URL");
        config.local_control_url = config.standard_control_url.clone();
        let (state, _telemetry_receiver) = GatewayState::new(&config).expect("gateway state");
        let token_lookup_hash = bot_public_id(&state.public_id_key, b"123:secret", false);
        state
            .install(RouteSnapshot {
                schema_version: 1,
                generation: 1,
                routes: vec![RouteRecord {
                    token_lookup_hash: token_lookup_hash.clone(),
                    pool: Pool::Standard,
                }],
            })
            .expect("install active route");
        let public_url = spawn_public_http1(public_router(state.clone())).await;
        let admin_url = spawn(admin_router(state.clone())).await;
        let client = Client::new();
        let drain_body = serde_json::json!({
            "schema_version": 1,
            "token_lookup_hash": token_lookup_hash,
            "minimum_snapshot_generation": 1,
            "bot_token": "123:secret",
            "telegram_test_dc": false,
        });

        let unauthorized = client
            .post(format!("{admin_url}/internal/routes/drain"))
            .json(&drain_body)
            .send()
            .await
            .expect("unauthorized drain response");
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
        assert!(unauthorized.bytes().await.expect("bounded error").len() < 256);

        let response = client
            .get(format!("{public_url}/bot123:secret/getUpdates?timeout=50"))
            .header(ROUTE_GENERATION_HEADER, "999")
            .send()
            .await
            .expect("admitted long-poll response");
        assert_eq!(response.status(), StatusCode::OK);
        let body_task = tokio::spawn(async move { response.bytes().await.expect("streamed body") });
        tokio::time::timeout(Duration::from_secs(2), first_chunk_rx.recv())
            .await
            .expect("first response chunk timeout")
            .expect("first response chunk");

        let present = request_drain(&client, &admin_url, &drain_body).await;
        assert_eq!(present["drained"], false);
        assert_eq!(present["snapshot_generation"], "1");
        assert_eq!(present["route_present"], true);
        assert_eq!(present["in_flight"], "1");
        assert_eq!(present["official_fenced"], false);

        state
            .install(RouteSnapshot {
                schema_version: 1,
                generation: 2,
                routes: vec![],
            })
            .expect("withdraw route");
        let mut withdrawn_body = drain_body.clone();
        withdrawn_body["minimum_snapshot_generation"] = 2.into();
        let blocked = request_drain(&client, &admin_url, &withdrawn_body).await;
        assert_eq!(blocked["drained"], false);
        assert_eq!(blocked["snapshot_generation"], "2");
        assert_eq!(blocked["route_present"], false);
        assert_eq!(blocked["in_flight"], "1");
        assert_eq!(blocked["official_fenced"], true);
        assert_eq!(blocked["official_active_requests"]["standard"], "1");
        assert_eq!(blocked["official_active_requests"]["local"], "1");

        // A replacement gateway has no process-local admission record, but
        // the same official process still owns the Query. Its proof remains
        // non-zero and therefore closes the restart hole.
        let (restarted, _restarted_telemetry) =
            GatewayState::new(&config).expect("restarted gateway state");
        restarted
            .install(RouteSnapshot {
                schema_version: 1,
                generation: 2,
                routes: vec![],
            })
            .expect("install withdrawn snapshot after restart");
        let restarted_admin_url = spawn(admin_router(restarted)).await;
        let restart_blocked = request_drain(&client, &restarted_admin_url, &withdrawn_body).await;
        assert_eq!(restart_blocked["in_flight"], "0");
        assert_eq!(restart_blocked["official_fenced"], true);
        assert_eq!(restart_blocked["official_active_requests"]["standard"], "1");
        assert_eq!(restart_blocked["drained"], false);

        let rejected = client
            .get(format!("{public_url}/bot123:secret/getMe"))
            .send()
            .await
            .expect("post-withdrawal response");
        assert_eq!(rejected.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(upstream_requests.load(Ordering::SeqCst), 1);

        release_body.add_permits(1);
        assert_eq!(
            body_task.await.expect("body task"),
            Bytes::from_static(b"first-last")
        );
        let drained = request_drain(&client, &admin_url, &withdrawn_body).await;
        assert_eq!(
            drained,
            serde_json::json!({
                "schema_version": 1,
                "drained": true,
                "snapshot_generation": "2",
                "route_present": false,
                "in_flight": "0",
                "official_fenced": true,
                "official_active_requests": {
                    "standard": "0",
                    "local": "0",
                },
            })
        );
        let restart_drained = request_drain(&client, &restarted_admin_url, &withdrawn_body).await;
        assert_eq!(restart_drained["drained"], true);
        assert!(
            !state
                .in_flight
                .lock()
                .expect("in-flight map")
                .contains_key(&token_lookup_hash)
        );

        let mut future_generation = withdrawn_body;
        future_generation["minimum_snapshot_generation"] = 3.into();
        let not_observed = request_drain(&client, &admin_url, &future_generation).await;
        assert_eq!(not_observed["drained"], false);
        assert_eq!(not_observed["snapshot_generation"], "2");
    }

    #[tokio::test]
    async fn streams_requests_and_responses_without_rewriting_telegram_semantics() {
        let (capture_tx, mut capture_rx) = mpsc::channel(2);
        let upstream = Router::new()
            .fallback(any(
                |State(capture_tx): State<mpsc::Sender<CapturedRequest>>,
                 request: Request| async move {
                    let (parts, body) = request.into_parts();
                    let captured = CapturedRequest {
                        method: parts.method,
                        path_and_query: parts
                            .uri
                            .path_and_query()
                            .expect("test request has a path")
                            .to_string(),
                        content_type: header_string(&parts.headers, header::CONTENT_TYPE),
                        content_length: header_string(&parts.headers, header::CONTENT_LENGTH),
                        removed_header: header_string(
                            &parts.headers,
                            HeaderName::from_static("x-remove"),
                        ),
                        body: to_bytes(body, 4 * 1024 * 1024)
                            .await
                            .expect("mock upstream reads body")
                            .to_vec(),
                    };
                    capture_tx.send(captured).await.expect("capture receiver");
                    Response::builder()
                        .status(StatusCode::CREATED)
                        .header("x-upstream", "preserved")
                        .header(header::CONNECTION, "x-strip")
                        .header("x-strip", "secret")
                        .body(Body::from("upstream-response"))
                        .expect("valid response")
                },
            ))
            .with_state(capture_tx);
        let upstream_url = spawn(upstream).await;
        let gateway_url = spawn_gateway(&upstream_url).await;
        let client = Client::new();

        let boundary = "phenogram-boundary";
        let multipart = format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"chat_id\"\r\n\r\n123\r\n--{boundary}--\r\n"
        );
        let response = client
            .post(format!(
                "{gateway_url}/bot123:secret/sendDocument?trace=one%2Ftwo"
            ))
            .header(
                header::CONTENT_TYPE,
                format!("multipart/form-data; boundary={boundary}"),
            )
            .header(header::CONNECTION, "x-remove")
            .header("x-remove", "must-not-reach-upstream")
            .body(multipart.clone())
            .send()
            .await
            .expect("gateway response");
        assert_eq!(response.status(), StatusCode::CREATED);
        assert_eq!(response.headers()["x-upstream"], "preserved");
        assert!(!response.headers().contains_key("x-strip"));
        assert_eq!(
            response.text().await.expect("response body"),
            "upstream-response"
        );

        let captured = capture_rx.recv().await.expect("captured request");
        assert_eq!(captured.method, Method::POST);
        assert_eq!(
            captured.path_and_query,
            "/bot123:secret/sendDocument?trace=one%2Ftwo"
        );
        assert_eq!(
            captured.content_type.as_deref(),
            Some("multipart/form-data; boundary=phenogram-boundary")
        );
        assert_eq!(
            captured.content_length.as_deref(),
            Some(multipart.len().to_string().as_str())
        );
        assert_eq!(captured.removed_header, None);
        assert_eq!(captured.body, multipart.as_bytes());

        let first = vec![b'a'; 1024 * 1024];
        let second = vec![b'b'; 1024 * 1024];
        let request_stream = futures_util::stream::iter(vec![
            Ok::<_, std::io::Error>(first.clone()),
            Ok::<_, std::io::Error>(second.clone()),
        ]);
        let response = client
            .post(format!("{gateway_url}/bot123:secret/sendDocument"))
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .body(reqwest::Body::wrap_stream(request_stream))
            .send()
            .await
            .expect("chunked gateway response");
        assert_eq!(response.status(), StatusCode::CREATED);

        let captured = capture_rx.recv().await.expect("captured chunked request");
        assert_eq!(captured.content_length, None);
        assert_eq!(captured.body.len(), first.len() + second.len());
        assert_eq!(&captured.body[..first.len()], first.as_slice());
        assert_eq!(&captured.body[first.len()..], second.as_slice());
    }

    #[tokio::test]
    async fn public_http1_accepts_official_sized_uri_and_more_than_one_hundred_headers() {
        let (capture_tx, mut capture_rx) = mpsc::channel(2);
        let upstream = Router::new()
            .fallback(any(
                |State(capture_tx): State<mpsc::Sender<(String, usize)>>,
                 request: Request| async move {
                    capture_tx
                        .send((
                            request
                                .uri()
                                .path_and_query()
                                .expect("upstream request target")
                                .to_string(),
                            request.headers().len(),
                        ))
                        .await
                        .expect("capture large request");
                    StatusCode::NO_CONTENT
                },
            ))
            .with_state(capture_tx);
        let upstream_url = spawn_public_http1(upstream).await;
        let gateway_url = spawn_gateway(&upstream_url).await;

        let long_target = format!("/bot123:secret/getMe?payload={}", "a".repeat(70 * 1024));
        assert!(long_target.len() > u16::MAX as usize);
        let response = raw_http_request(&gateway_url, &long_target, &[]).await;
        assert!(
            response.starts_with(b"HTTP/1.1 204"),
            "long target response was {}",
            String::from_utf8_lossy(&response[..response.len().min(80)])
        );
        let (captured_target, _) = capture_rx.recv().await.expect("captured long target");
        assert_eq!(captured_target, long_target);

        let header_lines = (0..128)
            .map(|index| format!("x-phenogram-{index:03}: value"))
            .collect::<Vec<_>>();
        let response = raw_http_request(&gateway_url, "/bot123:secret/getMe", &header_lines).await;
        assert!(
            response.starts_with(b"HTTP/1.1 204"),
            "large header-count response was {}",
            String::from_utf8_lossy(&response[..response.len().min(80)])
        );
        let (captured_target, captured_headers) =
            capture_rx.recv().await.expect("captured many headers");
        assert_eq!(captured_target, "/bot123:secret/getMe");
        assert!(
            captured_headers > 100,
            "captured {captured_headers} headers"
        );
    }

    #[tokio::test]
    async fn public_http1_accepts_official_header_count_and_name_bounds() {
        let (capture_tx, mut capture_rx) = mpsc::channel(2);
        let upstream = Router::new()
            .fallback(any(
                |State(capture_tx): State<mpsc::Sender<(usize, usize)>>,
                 request: Request| async move {
                    let longest_name = request
                        .headers()
                        .keys()
                        .map(|name| name.as_str().len())
                        .max()
                        .unwrap_or_default();
                    capture_tx
                        .send((request.headers().len(), longest_name))
                        .await
                        .expect("capture official-bound request headers");
                    StatusCode::NO_CONTENT
                },
            ))
            .with_state(capture_tx);
        let upstream_url = spawn_public_http1(upstream).await;
        let gateway_url = spawn_gateway(&upstream_url).await;

        // 25,000 distinct short fields fit below TDLib's 256 KiB request-head
        // limit and exceed the vendored HeaderMap's former 24,576-slot usable
        // capacity.
        let header_lines = (0..25_000)
            .map(|index| format!("x{index:04x}: v"))
            .collect::<Vec<_>>();
        let response = raw_http_request(&gateway_url, "/bot123:secret/getMe", &header_lines).await;
        assert!(
            response.starts_with(b"HTTP/1.1 204"),
            "high header-count response was {}",
            String::from_utf8_lossy(&response[..response.len().min(80)])
        );
        let (captured_headers, _) = capture_rx.recv().await.expect("captured 25,000 headers");
        assert!(
            captured_headers >= 25_000,
            "captured {captured_headers} headers"
        );

        let long_name = "a".repeat(70 * 1024);
        let response = raw_http_request(
            &gateway_url,
            "/bot123:secret/getMe",
            &[format!("{long_name}: v")],
        )
        .await;
        assert!(
            response.starts_with(b"HTTP/1.1 204"),
            "long header-name response was {}",
            String::from_utf8_lossy(&response[..response.len().min(80)])
        );
        let (_, captured_name_length) = capture_rx.recv().await.expect("captured long header name");
        assert_eq!(captured_name_length, long_name.len());
    }

    #[tokio::test]
    async fn rejects_malformed_api_and_file_paths_before_route_authentication() {
        let upstream = Router::new().fallback(any(|| async { StatusCode::NO_CONTENT }));
        let upstream_url = spawn(upstream).await;
        let gateway_url = spawn_gateway(&upstream_url).await;
        let client = Client::new();

        for path in [
            "/bot123:secret",
            "/bot123:secret/test",
            "/bot456:other",
            "/bot456:other/test",
            "/file/bot123:secret",
            "/file/bot123:secret/",
            "/file/bot123:secret/test/",
            "/file/bot456:other",
            "/file/bot456:other/",
        ] {
            let response = client
                .get(format!("{gateway_url}{path}"))
                .send()
                .await
                .expect("malformed path response");
            assert_eq!(response.status(), StatusCode::NOT_FOUND, "path: {path}");
        }

        let known_empty_method = client
            .get(format!("{gateway_url}/bot123:secret/"))
            .send()
            .await
            .expect("known structurally valid path");
        assert_eq!(known_empty_method.status(), StatusCode::NO_CONTENT);

        let unknown_valid = client
            .get(format!("{gateway_url}/bot456:other/getMe"))
            .send()
            .await
            .expect("unknown structurally valid path");
        assert_eq!(unknown_valid.status(), StatusCode::UNAUTHORIZED);

        let empty_token = client
            .get(format!("{gateway_url}/bot/getMe"))
            .send()
            .await
            .expect("empty-token response");
        assert_eq!(empty_token.status(), StatusCode::UNAUTHORIZED);

        for target in ["http://example.test/bot456:other/getMe", "*"] {
            let response = raw_http_request(&gateway_url, target, &[]).await;
            assert!(response.starts_with(b"HTTP/1.1 404"), "target: {target}");
            assert!(
                response
                    .windows(b"absolute URI is specified in the Request-Line".len())
                    .any(|window| window == b"absolute URI is specified in the Request-Line"),
                "target: {target}"
            );
        }
    }

    #[tokio::test]
    async fn authenticates_the_decoded_path_but_forwards_the_original_request_target() {
        let (capture_tx, mut capture_rx) = mpsc::channel(2);
        let upstream = Router::new()
            .fallback(any(
                |State(capture_tx): State<mpsc::Sender<String>>, request: Request| async move {
                    capture_tx
                        .send(
                            request
                                .uri()
                                .path_and_query()
                                .expect("upstream target")
                                .to_string(),
                        )
                        .await
                        .expect("capture target");
                    StatusCode::NO_CONTENT
                },
            ))
            .with_state(capture_tx);
        let upstream_url = spawn(upstream).await;
        let gateway_url = spawn_gateway(&upstream_url).await;
        let client = Client::new();

        let encoded_token = client
            .get(format!("{gateway_url}/bot123%3Asecret/getMe"))
            .send()
            .await
            .expect("encoded-token response");
        assert_eq!(encoded_token.status(), StatusCode::NO_CONTENT);
        assert_eq!(
            capture_rx.recv().await.expect("encoded-token target"),
            "/bot123%3Asecret/getMe"
        );

        let encoded_method_separator = client
            .get(format!("{gateway_url}/bot123:secret%2FgetMe"))
            .send()
            .await
            .expect("encoded-separator response");
        assert_eq!(encoded_method_separator.status(), StatusCode::NO_CONTENT);
        assert_eq!(
            capture_rx.recv().await.expect("encoded-separator target"),
            "/bot123:secret%2FgetMe"
        );

        let missing_test_separator = client
            .get(format!("{gateway_url}/bot123:secret%2Ftest"))
            .send()
            .await
            .expect("encoded malformed test path");
        assert_eq!(missing_test_separator.status(), StatusCode::NOT_FOUND);

        let encoded_token_separator = client
            .get(format!("{gateway_url}/bot123%2Fsecret/getMe"))
            .send()
            .await
            .expect("encoded token separator");
        assert_eq!(encoded_token_separator.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn production_and_test_dc_routes_are_independently_authenticated() {
        let upstream = Router::new().fallback(any(|| async { StatusCode::NO_CONTENT }));
        let upstream_url = spawn(upstream).await;
        let prod_gateway = spawn_gateway(&upstream_url).await;
        let test_gateway = spawn_gateway_with_file_upstream_for_route(
            &upstream_url,
            &upstream_url,
            Pool::Standard,
            true,
        )
        .await;
        let client = Client::new();

        assert_eq!(
            client
                .get(format!("{prod_gateway}/bot123:secret/test/getMe"))
                .send()
                .await
                .expect("test request against production route")
                .status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            client
                .get(format!("{test_gateway}/bot123:secret/getMe"))
                .send()
                .await
                .expect("production request against test route")
                .status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            client
                .get(format!("{test_gateway}/bot123:secret/test/getMe"))
                .send()
                .await
                .expect("test request against test route")
                .status(),
            StatusCode::NO_CONTENT
        );
    }

    #[tokio::test]
    async fn authenticated_file_bridge_streams_full_head_single_and_multiple_ranges() {
        let root = temporary_directory("file-bridge");
        let bot_directory = native_directory("123:secret", false);
        let file_directory = root.join(bot_directory).join("documents");
        tokio::fs::create_dir_all(&file_directory)
            .await
            .expect("create bot file directory");
        tokio::fs::write(file_directory.join("sample.bin"), b"0123456789")
            .await
            .expect("write bot file");

        let file_server_url = spawn(file_server_router(test_file_server_state(
            root.clone(),
            Pool::Standard,
        )))
        .await;
        let client = Client::new();
        let unauthenticated = client
            .get(format!(
                "{file_server_url}/file/bot123:secret/documents/sample.bin"
            ))
            .send()
            .await
            .expect("unauthenticated file response");
        assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            client
                .get(format!("{file_server_url}/health/live"))
                .send()
                .await
                .expect("unauthenticated sidecar health response")
                .status(),
            StatusCode::UNAUTHORIZED
        );

        let gateway_url =
            spawn_gateway_with_file_upstream("http://127.0.0.1:1", &file_server_url).await;
        let full = client
            .get(format!(
                "{gateway_url}/file/bot123%3Asecret/documents%2Fsample.bin"
            ))
            .send()
            .await
            .expect("full file response");
        assert_eq!(full.status(), StatusCode::OK);
        assert_eq!(full.headers()[header::ACCEPT_RANGES], "bytes");
        assert_eq!(full.headers()[header::CONTENT_LENGTH], "10");
        assert_eq!(
            full.bytes().await.expect("full file body"),
            &b"0123456789"[..]
        );

        let closed = client
            .get(format!(
                "{gateway_url}/file/bot123:secret/documents/sample.bin"
            ))
            .header(header::RANGE, "bytes=2-5")
            .send()
            .await
            .expect("closed range response");
        assert_eq!(closed.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(closed.headers()[header::CONTENT_RANGE], "bytes 2-5/10");
        assert_eq!(closed.headers()[header::CONTENT_LENGTH], "4");
        assert_eq!(
            closed.bytes().await.expect("closed range body"),
            &b"2345"[..]
        );

        let head = client
            .head(format!(
                "{gateway_url}/file/bot123:secret/documents/sample.bin"
            ))
            .header(header::RANGE, "bytes=-3")
            .send()
            .await
            .expect("head range response");
        assert_eq!(head.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(head.headers()[header::CONTENT_RANGE], "bytes 7-9/10");
        assert_eq!(head.headers()[header::CONTENT_LENGTH], "3");
        assert!(head.bytes().await.expect("head body").is_empty());

        let multiple = client
            .get(format!(
                "{gateway_url}/file/bot123:secret/documents/sample.bin"
            ))
            .header(header::RANGE, "bytes=0-1,8-")
            .send()
            .await
            .expect("multiple range response");
        assert_eq!(multiple.status(), StatusCode::PARTIAL_CONTENT);
        assert!(
            multiple.headers()[header::CONTENT_TYPE]
                .to_str()
                .expect("multipart content type")
                .starts_with("multipart/byteranges; boundary=phenogram-")
        );
        let multiple_body = multiple.bytes().await.expect("multiple range body");
        assert!(multiple_body.windows(2).any(|window| window == b"01"));
        assert!(multiple_body.windows(2).any(|window| window == b"89"));

        let invalid = client
            .get(format!(
                "{gateway_url}/file/bot123:secret/documents/sample.bin"
            ))
            .header(header::RANGE, "bytes=20-")
            .send()
            .await
            .expect("invalid range response");
        assert_eq!(invalid.status(), StatusCode::RANGE_NOT_SATISFIABLE);
        assert_eq!(invalid.headers()[header::CONTENT_RANGE], "bytes */10");

        let wrong_token = client
            .get(format!(
                "{gateway_url}/file/bot456:other/documents/sample.bin"
            ))
            .send()
            .await
            .expect("wrong token response");
        assert_eq!(wrong_token.status(), StatusCode::UNAUTHORIZED);

        let traversal = client
            .get(format!(
                "{gateway_url}/file/bot123:secret/%2e%2e%2Foutside.bin"
            ))
            .send()
            .await
            .expect("encoded traversal response");
        assert_eq!(traversal.status(), StatusCode::NOT_FOUND);

        tokio::fs::remove_dir_all(root)
            .await
            .expect("remove file test directory");
    }

    #[tokio::test]
    async fn file_bridge_exposes_only_pinned_tdlib_media_directories() {
        let root = temporary_directory("file-media-allowlist");
        let bot_directory = native_directory("123:secret", false);
        let bot_root = root.join(&bot_directory);
        tokio::fs::create_dir_all(&bot_root)
            .await
            .expect("create native bot directory");
        let state = test_file_server_state(root.clone(), Pool::Standard);

        for media_directory in TELEGRAM_MEDIA_DIRECTORIES {
            let relative = format!("{media_directory}/sample.bin");
            let file = bot_root.join(&relative);
            tokio::fs::create_dir_all(file.parent().expect("media parent"))
                .await
                .expect("create media directory");
            tokio::fs::write(&file, media_directory.as_bytes())
                .await
                .expect("write media file");
            let opened = open_requested_file(
                &state,
                FileRequestPath {
                    token: b"123:secret".to_vec(),
                    test_dc: false,
                    file_path: relative.into_bytes(),
                },
            )
            .await
            .expect("open pinned TDLib media file");
            assert_eq!(opened.length, media_directory.len() as u64);
        }

        for private_name in ["td.binlog", "db.sqlite", "td.sqlite"] {
            let private_file = bot_root.join(private_name);
            tokio::fs::write(&private_file, b"private TDLib state")
                .await
                .expect("write private state marker");
            let standard_result = open_requested_file(
                &state,
                FileRequestPath {
                    token: b"123:secret".to_vec(),
                    test_dc: false,
                    file_path: private_name.as_bytes().to_vec(),
                },
            )
            .await;
            assert!(matches!(standard_result, Err(SecureOpenError::NotFound)));

            #[cfg(unix)]
            {
                let local_state = test_file_server_state(root.clone(), Pool::Local);
                let local_result = open_requested_file(
                    &local_state,
                    FileRequestPath {
                        token: b"123:secret".to_vec(),
                        test_dc: false,
                        file_path: path_bytes(&private_file),
                    },
                )
                .await;
                assert!(matches!(local_result, Err(SecureOpenError::NotFound)));
            }
        }

        tokio::fs::remove_dir_all(root)
            .await
            .expect("remove allowlist test directory");
    }

    #[tokio::test]
    async fn local_mode_absolute_get_file_path_crosses_the_gateway_unchanged() {
        let root = temporary_directory("local-file-bridge");
        let bot_directory = native_directory("123:secret", false);
        let local_file = root.join(bot_directory).join("videos/file.bin");
        tokio::fs::create_dir_all(local_file.parent().expect("local file parent"))
            .await
            .expect("create local bot directory");
        tokio::fs::write(&local_file, b"local-mode")
            .await
            .expect("write local bot file");

        let file_server_url = spawn(file_server_router(test_file_server_state(
            root.clone(),
            Pool::Local,
        )))
        .await;
        let gateway_url = spawn_gateway_with_file_upstream_for_pool(
            "http://127.0.0.1:1",
            &file_server_url,
            Pool::Local,
        )
        .await;
        let response = Client::new()
            .get(format!(
                "{gateway_url}/file/bot123:secret/{}",
                local_file.to_string_lossy()
            ))
            .send()
            .await
            .expect("local absolute file response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.bytes().await.expect("local absolute body"),
            &b"local-mode"[..]
        );

        tokio::fs::remove_dir_all(root)
            .await
            .expect("remove local file test directory");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn file_bridge_rejects_traversal_symlinks_and_cross_bot_local_paths() {
        let root = temporary_directory("file-security");
        let first_directory = native_directory("123:secret", false);
        let second_directory = native_directory("456:other", false);
        let first_files = root.join(&first_directory).join("documents");
        let second_files = root.join(&second_directory).join("documents");
        tokio::fs::create_dir_all(&first_files)
            .await
            .expect("create first bot directory");
        tokio::fs::create_dir_all(&second_files)
            .await
            .expect("create second bot directory");
        tokio::fs::write(second_files.join("secret.bin"), b"other-bot")
            .await
            .expect("write second bot file");
        tokio::fs::write(root.join("outside.bin"), b"outside")
            .await
            .expect("write outside file");
        std::os::unix::fs::symlink(root.join("outside.bin"), first_files.join("escape.bin"))
            .expect("create escape symlink");

        let standard = test_file_server_state(root.clone(), Pool::Standard);
        for file_path in [
            format!("../{}/documents/secret.bin", second_directory.display()).into_bytes(),
            b"documents/escape.bin".to_vec(),
        ] {
            let result = open_requested_file(
                &standard,
                FileRequestPath {
                    token: b"123:secret".to_vec(),
                    test_dc: false,
                    file_path,
                },
            )
            .await;
            assert!(matches!(result, Err(SecureOpenError::NotFound)));
        }

        let local = test_file_server_state(root.clone(), Pool::Local);
        let valid_path = first_files.join("local.bin");
        tokio::fs::write(&valid_path, b"local")
            .await
            .expect("write local file");
        let valid = open_requested_file(
            &local,
            FileRequestPath {
                token: b"123:secret".to_vec(),
                test_dc: false,
                file_path: path_bytes(&valid_path),
            },
        )
        .await
        .expect("open exact local path");
        assert_eq!(valid.length, 5);
        let normalized = open_requested_file(
            &local,
            FileRequestPath {
                token: b"123:secret".to_vec(),
                test_dc: false,
                file_path: path_bytes(
                    valid_path
                        .strip_prefix(Path::new("/"))
                        .expect("absolute local path"),
                ),
            },
        )
        .await
        .expect("open ingress-normalized local path");
        assert_eq!(normalized.length, 5);

        let cross_bot = open_requested_file(
            &local,
            FileRequestPath {
                token: b"123:secret".to_vec(),
                test_dc: false,
                file_path: path_bytes(&second_files.join("secret.bin")),
            },
        )
        .await;
        assert!(matches!(cross_bot, Err(SecureOpenError::NotFound)));
        assert_ne!(
            native_directory("123:secret", false),
            native_directory("123:secret", true)
        );

        tokio::fs::remove_dir_all(root)
            .await
            .expect("remove security test directory");
    }

    #[tokio::test]
    async fn a_transport_failure_is_not_retried() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind raw server");
        let upstream_url = format!("http://{}", listener.local_addr().expect("raw address"));
        let accepted = Arc::new(AtomicUsize::new(0));
        let accepted_by_server = accepted.clone();
        tokio::spawn(async move {
            while let Ok(Ok((socket, _))) =
                tokio::time::timeout(Duration::from_millis(500), listener.accept()).await
            {
                accepted_by_server.fetch_add(1, Ordering::SeqCst);
                drop(socket);
            }
        });
        let gateway_url = spawn_gateway(&upstream_url).await;

        let response = Client::new()
            .post(format!("{gateway_url}/bot123:secret/sendMessage"))
            .body("payload")
            .send()
            .await
            .expect("gateway returns a Telegram-shaped transport error");
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        tokio::time::sleep(Duration::from_millis(600)).await;
        assert_eq!(accepted.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn telemetry_is_batched_and_bearer_authenticated_off_the_request_path() {
        #[derive(Debug)]
        struct CapturedBatch {
            authorization: Option<String>,
            body: serde_json::Value,
        }

        let (capture_tx, mut capture_rx) = mpsc::channel(1);
        let telemetry = Router::new()
            .fallback(any(
                |State(capture_tx): State<mpsc::Sender<CapturedBatch>>,
                 request: Request| async move {
                    let (parts, body) = request.into_parts();
                    let captured = CapturedBatch {
                        authorization: header_string(&parts.headers, header::AUTHORIZATION),
                        body: serde_json::from_slice(
                            &to_bytes(body, 1024 * 1024)
                                .await
                                .expect("telemetry body"),
                        )
                        .expect("telemetry JSON"),
                    };
                    capture_tx.send(captured).await.expect("capture batch");
                    StatusCode::NO_CONTENT
                },
            ))
            .with_state(capture_tx);
        let telemetry_url = format!(
            "{}/api/internal/data-plane/telemetry",
            spawn(telemetry).await
        );
        let config = test_config("http://127.0.0.1:1", &telemetry_url);
        let (state, receiver) = GatewayState::new(&config).expect("gateway state");
        tokio::spawn(telemetry_delivery_loop(state.clone(), config, receiver));

        for status in [StatusCode::OK, StatusCode::CREATED, StatusCode::BAD_GATEWAY] {
            state.record_telemetry(telemetry_event(
                "phg_012345678901234567890123".into(),
                Pool::Standard,
                "sendMessage".into(),
                status,
                Duration::from_millis(7),
            ));
        }
        state.record_telemetry(TelemetryEvent::OutboundMessage(Box::new(
            OutboundMessageTelemetryEvent {
                schema_version: 1,
                kind: "outbound_message",
                token_lookup_hash: "phg_012345678901234567890123".into(),
                pool: Pool::Standard,
                method: "sendPhoto".into(),
                upstream_status: 200,
                observed_at_unix_us: 1_786_620_000_000_000,
                message: ObservedOutboundMessage {
                    chat_id: 99,
                    telegram_message_id: Some(9002),
                    receiver_user_id: None,
                    ephemeral_message_id: None,
                    business_connection_id: None,
                    guest_query_id: None,
                    message_thread_id: None,
                    direct_messages_topic_id: None,
                    text: Some("caption".into()),
                    chat_type: Some("private".into()),
                    title: None,
                    username: Some("ada".into()),
                    display_name: Some("Ada".into()),
                    payload: Some(Box::new(serde_json::json!({
                        "message_id": 9002,
                        "chat": {"id": 99, "type": "private"},
                        "photo": [{"file_id": "photo", "file_unique_id": "unique"}]
                    }))),
                },
                _queue_permit: None,
            },
        )));

        let captured = tokio::time::timeout(Duration::from_secs(2), capture_rx.recv())
            .await
            .expect("telemetry delivery timeout")
            .expect("captured telemetry batch");
        assert_eq!(
            captured.authorization.as_deref(),
            Some(format!("Bearer {}", "s".repeat(32)).as_str())
        );
        assert_eq!(captured.body["schema_version"], 1);
        assert_eq!(
            captured.body["events"]
                .as_array()
                .expect("events array")
                .len(),
            4
        );
        assert_eq!(captured.body["events"][3]["kind"], "outbound_message");
        assert_eq!(captured.body["events"][3]["message"]["chat_id"], 99);
        tokio::time::timeout(Duration::from_secs(1), async {
            while state.telemetry_metrics.delivered.load(Ordering::Relaxed) != 4 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("delivery metric update");
        assert_eq!(state.telemetry_metrics.dropped.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn retryable_telemetry_failure_keeps_the_exact_batch_until_delivery() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let (capture_tx, mut capture_rx) = mpsc::channel(1);
        let telemetry = Router::new()
            .fallback(any(
                |State((attempts, capture_tx)): State<(
                    Arc<AtomicUsize>,
                    mpsc::Sender<serde_json::Value>,
                )>,
                 request: Request| async move {
                    let body = serde_json::from_slice::<serde_json::Value>(
                        &to_bytes(request.into_body(), 1024 * 1024)
                            .await
                            .expect("telemetry body"),
                    )
                    .expect("telemetry JSON");
                    if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                        return StatusCode::SERVICE_UNAVAILABLE;
                    }
                    capture_tx.send(body).await.expect("capture retry");
                    StatusCode::NO_CONTENT
                },
            ))
            .with_state((attempts.clone(), capture_tx));
        let telemetry_url = format!(
            "{}/api/internal/data-plane/telemetry",
            spawn(telemetry).await
        );
        let config = test_config("http://127.0.0.1:1", &telemetry_url);
        let (state, receiver) = GatewayState::new(&config).expect("gateway state");
        tokio::spawn(telemetry_delivery_loop(state.clone(), config, receiver));

        state.record_telemetry(telemetry_event(
            "phg_012345678901234567890123".into(),
            Pool::Standard,
            "sendMessage".into(),
            StatusCode::OK,
            Duration::from_millis(7),
        ));

        let captured = tokio::time::timeout(Duration::from_secs(2), capture_rx.recv())
            .await
            .expect("telemetry retry timeout")
            .expect("captured retried batch");
        assert_eq!(captured["events"].as_array().map(Vec::len), Some(1));
        tokio::time::timeout(Duration::from_secs(1), async {
            while state.telemetry_metrics.delivered.load(Ordering::Relaxed) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("delivery metric update after retry");
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert_eq!(
            state
                .telemetry_metrics
                .delivery_failed
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(state.telemetry_metrics.delivered.load(Ordering::Relaxed), 1);
    }

    async fn raw_http_request(base_url: &str, target: &str, header_lines: &[String]) -> Vec<u8> {
        let url = Url::parse(base_url).expect("raw HTTP base URL");
        let host = url.host_str().expect("raw HTTP host");
        let port = url.port_or_known_default().expect("raw HTTP port");
        let mut stream = TcpStream::connect((host, port))
            .await
            .expect("connect raw HTTP client");
        let mut request = format!("GET {target} HTTP/1.1\r\nHost: {host}:{port}\r\n");
        for line in header_lines {
            request.push_str(line);
            request.push_str("\r\n");
        }
        request.push_str("Connection: close\r\n\r\n");
        stream
            .write_all(request.as_bytes())
            .await
            .expect("write raw HTTP request");
        let mut response = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut response))
            .await
            .expect("raw HTTP response timeout")
            .expect("read raw HTTP response");
        response
    }

    async fn request_drain(
        client: &Client,
        admin_url: &str,
        body: &serde_json::Value,
    ) -> serde_json::Value {
        let response = client
            .post(format!("{admin_url}/internal/routes/drain"))
            .bearer_auth("s".repeat(32))
            .json(body)
            .send()
            .await
            .expect("drain response");
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = response.bytes().await.expect("drain response body");
        assert!(bytes.len() < 512, "drain response must stay bounded");
        serde_json::from_slice(&bytes).expect("drain response JSON")
    }

    async fn spawn(router: Router) -> String {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test server");
        let address = listener.local_addr().expect("test server address");
        tokio::spawn(async move {
            axum::serve(listener, router)
                .await
                .expect("test server runs");
        });
        format!("http://{address}")
    }

    async fn spawn_gateway(upstream_url: &str) -> String {
        spawn_gateway_with_file_upstream(upstream_url, upstream_url).await
    }

    async fn spawn_gateway_with_file_upstream(
        upstream_url: &str,
        file_upstream_url: &str,
    ) -> String {
        spawn_gateway_with_file_upstream_for_pool(upstream_url, file_upstream_url, Pool::Standard)
            .await
    }

    async fn spawn_gateway_with_file_upstream_for_pool(
        upstream_url: &str,
        file_upstream_url: &str,
        pool: Pool,
    ) -> String {
        spawn_gateway_with_file_upstream_for_route(upstream_url, file_upstream_url, pool, false)
            .await
    }

    async fn spawn_gateway_with_file_upstream_for_route(
        upstream_url: &str,
        file_upstream_url: &str,
        pool: Pool,
        test_dc: bool,
    ) -> String {
        let mut config = test_config(
            upstream_url,
            "http://127.0.0.1/api/internal/data-plane/telemetry",
        );
        config.standard_file_upstream = file_upstream_url.to_string();
        config.local_file_upstream = file_upstream_url.to_string();
        let (state, _telemetry_receiver) = GatewayState::new(&config).expect("gateway state");
        let token_lookup_hash = bot_public_id(&state.public_id_key, b"123:secret", test_dc);
        state
            .install(RouteSnapshot {
                schema_version: 1,
                generation: 1,
                routes: vec![RouteRecord {
                    token_lookup_hash,
                    pool,
                }],
            })
            .expect("install test routes");
        spawn_public_http1(public_router(state)).await
    }

    async fn spawn_public_http1(router: Router) -> String {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind public HTTP/1 test server");
        let address = listener.local_addr().expect("public HTTP/1 address");
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        tokio::spawn(async move {
            let result = serve_public_http1(listener, router, shutdown_rx).await;
            drop(shutdown_tx);
            result.expect("public HTTP/1 test server runs");
        });
        format!("http://{address}")
    }

    fn native_directory(token: &str, test_dc: bool) -> PathBuf {
        native_bot_directories(token.as_bytes(), test_dc)
            .expect("native bot directories")
            .into_iter()
            .next()
            .expect("native directory")
    }

    #[cfg(unix)]
    fn path_bytes(path: &Path) -> Vec<u8> {
        use std::os::unix::ffi::OsStrExt;

        path.as_os_str().as_bytes().to_vec()
    }

    fn test_file_server_state(root: PathBuf, pool: Pool) -> FileServerState {
        FileServerState {
            root,
            pool,
            sync_token_digest: Sha256::digest("s".repeat(32).as_bytes()).into(),
        }
    }

    fn temporary_directory(label: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "phenogram-gateway-{label}-{}-{unique}",
            std::process::id()
        ))
    }

    fn test_config(upstream_url: &str, telemetry_url: &str) -> Config {
        Config {
            public_listen_addr: "127.0.0.1:0".parse().expect("public address"),
            admin_listen_addr: "127.0.0.1:0".parse().expect("admin address"),
            standard_upstream: upstream_url.to_string(),
            local_upstream: upstream_url.to_string(),
            standard_file_upstream: upstream_url.to_string(),
            local_file_upstream: upstream_url.to_string(),
            standard_control_url: Url::parse("http://127.0.0.1/internal/official/drain")
                .expect("standard control URL"),
            local_control_url: Url::parse("http://127.0.0.1/internal/official/drain")
                .expect("local control URL"),
            snapshot_url: Url::parse("http://127.0.0.1/internal/v1/data-plane/routes")
                .expect("snapshot URL"),
            snapshot_path: PathBuf::from("/unused/routes.json"),
            snapshot_refresh_interval: Duration::from_secs(5),
            telemetry_url: Url::parse(telemetry_url).expect("telemetry URL"),
            telemetry_queue_capacity: 100,
            public_id_key: "b".repeat(32),
            sync_token: "s".repeat(32),
        }
    }

    fn test_outbound_observation(state: &GatewayState) -> OutboundObservation {
        state
            .admit_outbound_observation(
                "phg_012345678901234567890123".into(),
                Pool::Standard,
                "sendMessage".into(),
                StatusCode::OK,
            )
            .expect("outbound observation slot")
    }

    fn header_string(headers: &HeaderMap, name: HeaderName) -> Option<String> {
        headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .map(ToOwned::to_owned)
    }
}
