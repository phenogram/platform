use std::{
    collections::HashMap,
    env,
    ffi::OsStr,
    fs,
    net::SocketAddr,
    os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use axum::{
    Router,
    extract::{Request, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::post,
};
use futures_util::StreamExt as _;
use reqwest::Url;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, postgres::PgPoolOptions};
use subtle::ConstantTimeEq as _;
use tokio::{net::UnixDatagram, sync::mpsc};
use uuid::Uuid;

use crate::{
    error::{AppError, Result},
    ingestion::{
        IngestionBot, IngestionOutcome, IngestionSource, ManagedLifecycleDelivery,
        ManagedLifecycleOutcome, ingest_managed_bot_lifecycle, ingest_update,
    },
};

pub const DEFAULT_SOCKET_PATH: &str = "/run/phenogram-tap/tap.sock";
pub const DEFAULT_ACK_SOCKET_PATH: &str = "/run/phenogram-tap/ack.sock";
pub const MAX_DATAGRAM_BYTES: usize = 32_768;
pub const HEADER_BYTES: usize = 64;
pub const MAX_FRAGMENT_BYTES: usize = MAX_DATAGRAM_BYTES - HEADER_BYTES;
pub const PROTOCOL_MAX_EVENT_BYTES: usize = 262_144;
pub const PROTOCOL_MAX_FRAGMENTS: usize = 9;
const SOCKET_RECEIVE_BUFFER_BYTES: usize = 1024 * 1024;
const MAGIC: &[u8; 4] = b"PGUT";
const ACK_MAGIC: &[u8; 4] = b"PGUA";
const VERSION: u8 = 1;
const UPDATE_FRAME: u8 = 1;
const MANAGED_BOT_LIFECYCLE_FRAME: u8 = 2;
const FLAG_TEST_DC: u8 = 1;
const DATABASE_WORKERS: usize = 4;
const DATABASE_OPERATION_TIMEOUT: Duration = Duration::from_secs(3);
const MAX_OFFICIAL_STATS_BYTES: usize = 64 * 1024;
const MAX_OFFICIAL_CONTROL_REQUEST_BYTES: usize = 1024;
const MAX_OFFICIAL_CONTROL_RESPONSE_BYTES: usize = 1024;
const OFFICIAL_STATS_NAMES: [&str; 4] = [
    "managed_lifecycle_overflow",
    "managed_lifecycle_persistence_errors",
    "managed_lifecycle_expired",
    "managed_lifecycle_ack_errors",
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TapPool {
    Standard,
    Local,
}

impl TapPool {
    fn from_config_value(value: Option<&str>) -> std::result::Result<Self, String> {
        match value {
            Some("standard") => Ok(Self::Standard),
            Some("local") => Ok(Self::Local),
            _ => Err("PHENOGRAM_TAP_POOL is required and must be exactly standard or local".into()),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Standard => "standard",
            Self::Local => "local",
        }
    }
}

#[derive(Clone, Debug)]
pub struct TapConfig {
    pub database_url: String,
    pub pool: TapPool,
    pub socket_path: PathBuf,
    pub ack_socket_path: PathBuf,
    pub max_event_bytes: usize,
    pub max_inflight_events: usize,
    pub max_inflight_bytes: usize,
    pub reassembly_timeout: Duration,
    pub official_stats_url: Option<Url>,
    pub official_stats_interval: Duration,
    pub official_control_listen_addr: Option<SocketAddr>,
    pub official_control_token_digest: Option<[u8; 32]>,
}

impl TapConfig {
    pub fn from_env() -> std::result::Result<Self, String> {
        let database_url = env::var("DATABASE_URL")
            .map_err(|_| "DATABASE_URL is required for the tap collector".to_owned())?;
        let pool_value = env::var("PHENOGRAM_TAP_POOL").ok();
        let pool = TapPool::from_config_value(pool_value.as_deref())?;
        let socket_path = env::var_os("PHENOGRAM_TAP_SOCKET_PATH")
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_SOCKET_PATH));
        if !socket_path.is_absolute()
            || socket_path
                .file_name()
                .is_none_or(|name| name == OsStr::new(""))
        {
            return Err("PHENOGRAM_TAP_SOCKET_PATH must be an absolute socket path".into());
        }
        let ack_socket_path = env::var_os("PHENOGRAM_TAP_ACK_SOCKET_PATH")
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_ACK_SOCKET_PATH));
        if !ack_socket_path.is_absolute()
            || ack_socket_path
                .file_name()
                .is_none_or(|name| name == OsStr::new(""))
            || ack_socket_path == socket_path
        {
            return Err(
                "PHENOGRAM_TAP_ACK_SOCKET_PATH must be an absolute path distinct from the tap socket"
                    .into(),
            );
        }
        let max_event_bytes = parse_bound(
            "PHENOGRAM_TAP_MAX_EVENT_BYTES",
            PROTOCOL_MAX_EVENT_BYTES,
            1,
            PROTOCOL_MAX_EVENT_BYTES,
        )?;
        let max_inflight_events =
            parse_bound("PHENOGRAM_TAP_MAX_INFLIGHT_EVENTS", 1_024, 1, 65_536)?;
        let max_inflight_bytes = parse_bound(
            "PHENOGRAM_TAP_MAX_INFLIGHT_BYTES",
            64 * 1024 * 1024,
            max_event_bytes,
            1024 * 1024 * 1024,
        )?;
        let timeout_ms = parse_bound("PHENOGRAM_TAP_REASSEMBLY_TIMEOUT_MS", 5_000, 100, 60_000)?;
        let official_stats_url = parse_official_stats_url(
            env::var("PHENOGRAM_TAP_OFFICIAL_STATS_URL")
                .ok()
                .filter(|value| !value.trim().is_empty()),
        )?;
        let official_stats_interval = Duration::from_secs(parse_bound(
            "PHENOGRAM_TAP_OFFICIAL_STATS_INTERVAL_SECONDS",
            15,
            1,
            300,
        )? as u64);
        let official_control_listen_addr = env::var("PHENOGRAM_TAP_CONTROL_LISTEN_ADDR")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .map(|value| {
                value
                    .parse::<SocketAddr>()
                    .map_err(|_| "PHENOGRAM_TAP_CONTROL_LISTEN_ADDR must be a socket address")
            })
            .transpose()?;
        let official_control_token_digest = if official_control_listen_addr.is_some() {
            if official_stats_url.is_none() {
                return Err(
                    "PHENOGRAM_TAP_OFFICIAL_STATS_URL is required when the control listener is enabled"
                        .into(),
                );
            }
            let token = env::var("DATA_PLANE_SYNC_TOKEN").map_err(|_| {
                "DATA_PLANE_SYNC_TOKEN is required when the tap control listener is enabled"
                    .to_string()
            })?;
            if token.len() < 32 {
                return Err("DATA_PLANE_SYNC_TOKEN must contain at least 32 characters".into());
            }
            Some(Sha256::digest(token.as_bytes()).into())
        } else {
            None
        };
        Ok(Self {
            database_url,
            pool,
            socket_path,
            ack_socket_path,
            max_event_bytes,
            max_inflight_events,
            max_inflight_bytes,
            reassembly_timeout: Duration::from_millis(timeout_ms as u64),
            official_stats_url,
            official_stats_interval,
            official_control_listen_addr,
            official_control_token_digest,
        })
    }
}

fn parse_official_stats_url(value: Option<String>) -> std::result::Result<Option<Url>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    let url = Url::parse(&value)
        .map_err(|_| "PHENOGRAM_TAP_OFFICIAL_STATS_URL must be a valid URL".to_string())?;
    if url.scheme() != "http"
        || url.host_str() != Some("127.0.0.1")
        || url.port() != Some(8083)
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err(
            "PHENOGRAM_TAP_OFFICIAL_STATS_URL must be exactly http://127.0.0.1:8083/".into(),
        );
    }
    Ok(Some(url))
}

fn parse_bound(
    name: &str,
    default: usize,
    minimum: usize,
    maximum: usize,
) -> std::result::Result<usize, String> {
    let value = match env::var(name) {
        Ok(value) if !value.trim().is_empty() => value
            .parse::<usize>()
            .map_err(|_| format!("{name} must be an integer"))?,
        _ => default,
    };
    if !(minimum..=maximum).contains(&value) {
        return Err(format!("{name} must be between {minimum} and {maximum}"));
    }
    Ok(value)
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct EventKey {
    producer_instance_id: u64,
    event_sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct EventMetadata {
    frame_type: FrameType,
    test_dc: bool,
    bot_id: i64,
    update_id: i64,
    expires_at: u32,
    total_payload_len: usize,
    fragment_count: usize,
    lifecycle_delivery_nonce: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FrameType {
    Update,
    ManagedBotLifecycle,
}

#[derive(Debug)]
struct Fragment {
    key: EventKey,
    metadata: EventMetadata,
    index: usize,
    offset: usize,
    payload: Vec<u8>,
}

#[derive(Debug)]
struct FragmentPart {
    offset: usize,
    payload: Vec<u8>,
}

#[derive(Debug)]
struct PartialEvent {
    metadata: EventMetadata,
    first_seen: Instant,
    fragments: Vec<Option<FragmentPart>>,
}

#[derive(Debug)]
struct CompletedEvent {
    key: EventKey,
    metadata: EventMetadata,
    member_json: Vec<u8>,
}

#[derive(Debug, thiserror::Error)]
enum FrameError {
    #[error("datagram has an invalid length")]
    DatagramLength,
    #[error("frame signature or version is unsupported")]
    Protocol,
    #[error("frame contains unsupported flags or non-zero reserved bytes")]
    Reserved,
    #[error("frame length fields are inconsistent")]
    Length,
    #[error("frame bot id is outside the supported range")]
    BotId,
    #[error("frame fragment coordinates are invalid")]
    Fragment,
    #[error("event exceeds the configured reassembly limit")]
    EventTooLarge,
    #[error("fragment metadata changed within an event")]
    MetadataChanged,
    #[error("duplicate fragment contains different bytes")]
    ConflictingDuplicate,
    #[error("complete event has a gap or overlap")]
    Coverage,
    #[error("reassembly capacity is exhausted")]
    Capacity,
    #[error("reassembled update is not valid canonical JSON")]
    Json,
}

#[derive(Debug)]
struct Reassembler {
    events: HashMap<EventKey, PartialEvent>,
    reserved_bytes: usize,
    max_event_bytes: usize,
    max_inflight_events: usize,
    max_inflight_bytes: usize,
    timeout: Duration,
}

impl Reassembler {
    fn new(config: &TapConfig) -> Self {
        Self {
            events: HashMap::new(),
            reserved_bytes: 0,
            max_event_bytes: config.max_event_bytes,
            max_inflight_events: config.max_inflight_events,
            max_inflight_bytes: config.max_inflight_bytes,
            timeout: config.reassembly_timeout,
        }
    }

    fn accept(
        &mut self,
        datagram: &[u8],
        now: Instant,
    ) -> std::result::Result<Option<CompletedEvent>, FrameError> {
        let fragment = parse_fragment(datagram, self.max_event_bytes)?;
        if !self.events.contains_key(&fragment.key) {
            if self.events.len() >= self.max_inflight_events
                || self
                    .reserved_bytes
                    .checked_add(fragment.metadata.total_payload_len)
                    .is_none_or(|bytes| bytes > self.max_inflight_bytes)
            {
                return Err(FrameError::Capacity);
            }
            self.reserved_bytes += fragment.metadata.total_payload_len;
            self.events.insert(
                fragment.key,
                PartialEvent {
                    fragments: (0..fragment.metadata.fragment_count)
                        .map(|_| None)
                        .collect(),
                    metadata: fragment.metadata.clone(),
                    first_seen: now,
                },
            );
        }

        let event = self
            .events
            .get_mut(&fragment.key)
            .expect("event was inserted above");
        if event.metadata != fragment.metadata {
            self.remove(fragment.key);
            return Err(FrameError::MetadataChanged);
        }
        if let Some(existing) = &event.fragments[fragment.index] {
            if existing.offset == fragment.offset && existing.payload == fragment.payload {
                return Ok(None);
            }
            self.remove(fragment.key);
            return Err(FrameError::ConflictingDuplicate);
        }
        event.fragments[fragment.index] = Some(FragmentPart {
            offset: fragment.offset,
            payload: fragment.payload,
        });
        if event.fragments.iter().any(Option::is_none) {
            return Ok(None);
        }

        let key = fragment.key;
        let event = self.remove(key).expect("complete event exists");
        let mut cursor = 0_usize;
        let mut member_json = Vec::with_capacity(event.metadata.total_payload_len);
        for part in event.fragments.into_iter().flatten() {
            if part.offset != cursor {
                return Err(FrameError::Coverage);
            }
            cursor = cursor
                .checked_add(part.payload.len())
                .ok_or(FrameError::Coverage)?;
            member_json.extend_from_slice(&part.payload);
        }
        if cursor != event.metadata.total_payload_len {
            return Err(FrameError::Coverage);
        }
        Ok(Some(CompletedEvent {
            key,
            metadata: event.metadata,
            member_json,
        }))
    }

    fn purge_expired(&mut self, now: Instant) -> usize {
        let expired = self
            .events
            .iter()
            .filter_map(|(key, event)| {
                now.saturating_duration_since(event.first_seen)
                    .ge(&self.timeout)
                    .then_some(*key)
            })
            .collect::<Vec<_>>();
        for key in &expired {
            self.remove(*key);
        }
        expired.len()
    }

    fn remove(&mut self, key: EventKey) -> Option<PartialEvent> {
        let event = self.events.remove(&key)?;
        self.reserved_bytes = self
            .reserved_bytes
            .saturating_sub(event.metadata.total_payload_len);
        Some(event)
    }
}

fn parse_fragment(
    datagram: &[u8],
    max_event_bytes: usize,
) -> std::result::Result<Fragment, FrameError> {
    if !(HEADER_BYTES..=MAX_DATAGRAM_BYTES).contains(&datagram.len()) {
        return Err(FrameError::DatagramLength);
    }
    if &datagram[0..4] != MAGIC || datagram[4] != VERSION || datagram[7] as usize != HEADER_BYTES {
        return Err(FrameError::Protocol);
    }
    let frame_type = match datagram[5] {
        UPDATE_FRAME => FrameType::Update,
        MANAGED_BOT_LIFECYCLE_FRAME => FrameType::ManagedBotLifecycle,
        _ => return Err(FrameError::Protocol),
    };
    let flags = datagram[6];
    if flags & !FLAG_TEST_DC != 0 {
        return Err(FrameError::Reserved);
    }
    let producer_instance_id = read_u64(datagram, 8);
    let event_sequence = read_u64(datagram, 16);
    let bot_id = read_u64(datagram, 24);
    let bot_id = i64::try_from(bot_id)
        .ok()
        .filter(|bot_id| *bot_id > 0)
        .ok_or(FrameError::BotId)?;
    let update_id = i64::from(read_u32(datagram, 32));
    let expires_at = read_u32(datagram, 36);
    let total_payload_len = read_u32(datagram, 40) as usize;
    let fragment_offset = read_u32(datagram, 44) as usize;
    let fragment_index = read_u16(datagram, 48) as usize;
    let fragment_count = read_u16(datagram, 50) as usize;
    let fragment_len = read_u32(datagram, 52) as usize;
    let lifecycle_delivery_nonce = read_u64(datagram, 56);
    match frame_type {
        FrameType::Update => {
            if lifecycle_delivery_nonce != 0 {
                return Err(FrameError::Reserved);
            }
            if total_payload_len > PROTOCOL_MAX_EVENT_BYTES || total_payload_len > max_event_bytes {
                return Err(FrameError::EventTooLarge);
            }
            if fragment_count == 0
                || fragment_count > PROTOCOL_MAX_FRAGMENTS
                || fragment_index >= fragment_count
                || fragment_len > MAX_FRAGMENT_BYTES
            {
                return Err(FrameError::Fragment);
            }
            if fragment_len == 0
                && !(total_payload_len == 0
                    && fragment_count == 1
                    && fragment_index == 0
                    && fragment_offset == 0)
            {
                return Err(FrameError::Fragment);
            }
        }
        FrameType::ManagedBotLifecycle => {
            if update_id <= 0
                || expires_at == 0
                || producer_instance_id == 0
                || event_sequence == 0
                || lifecycle_delivery_nonce == 0
                || total_payload_len != 16
                || fragment_offset != 0
                || fragment_index != 0
                || fragment_count != 1
                || fragment_len != 16
            {
                return Err(FrameError::Fragment);
            }
        }
    }
    if datagram.len() != HEADER_BYTES + fragment_len {
        return Err(FrameError::Length);
    }
    if fragment_offset
        .checked_add(fragment_len)
        .is_none_or(|end| end > total_payload_len)
    {
        return Err(FrameError::Fragment);
    }
    Ok(Fragment {
        key: EventKey {
            producer_instance_id,
            event_sequence,
        },
        metadata: EventMetadata {
            frame_type,
            test_dc: flags & FLAG_TEST_DC != 0,
            bot_id,
            update_id,
            expires_at,
            total_payload_len,
            fragment_count,
            lifecycle_delivery_nonce,
        },
        index: fragment_index,
        offset: fragment_offset,
        payload: datagram[HEADER_BYTES..].to_vec(),
    })
}

fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_be_bytes(bytes[offset..offset + 2].try_into().expect("fixed header"))
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes(bytes[offset..offset + 4].try_into().expect("fixed header"))
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_be_bytes(bytes[offset..offset + 8].try_into().expect("fixed header"))
}

fn decode_event(event: CompletedEvent) -> std::result::Result<TapEvent, FrameError> {
    match event.metadata.frame_type {
        FrameType::Update => {
            let mut json = Vec::with_capacity(event.member_json.len() + 40);
            json.extend_from_slice(
                format!("{{\"update_id\":{},", event.metadata.update_id).as_bytes(),
            );
            json.extend_from_slice(&event.member_json);
            json.push(b'}');
            let payload = serde_json::from_slice::<Value>(&json).map_err(|_| FrameError::Json)?;
            if !payload.is_object()
                || payload.get("update_id").and_then(Value::as_i64)
                    != Some(event.metadata.update_id)
            {
                return Err(FrameError::Json);
            }
            Ok(TapEvent::Update(TapUpdate {
                test_dc: event.metadata.test_dc,
                telegram_bot_id: event.metadata.bot_id,
                update_id: event.metadata.update_id,
                expires_at: event.metadata.expires_at,
                payload,
            }))
        }
        FrameType::ManagedBotLifecycle => {
            if event.member_json.len() != 16 {
                return Err(FrameError::Length);
            }
            let managed_owner_telegram_user_id = i64::try_from(read_u64(&event.member_json, 0))
                .ok()
                .filter(|id| *id > 0)
                .ok_or(FrameError::BotId)?;
            let managed_telegram_bot_id = i64::try_from(read_u64(&event.member_json, 8))
                .ok()
                .filter(|id| *id > 0)
                .ok_or(FrameError::BotId)?;
            Ok(TapEvent::ManagedBotLifecycle(ManagedBotLifecycle {
                producer_instance_id: event.key.producer_instance_id,
                event_sequence: event.key.event_sequence,
                test_dc: event.metadata.test_dc,
                parent_telegram_bot_id: event.metadata.bot_id,
                observer_event_id: event.metadata.update_id,
                expires_at: event.metadata.expires_at,
                delivery_nonce: event.metadata.lifecycle_delivery_nonce,
                managed_owner_telegram_user_id,
                managed_telegram_bot_id,
            }))
        }
    }
}

#[derive(Debug)]
enum TapEvent {
    Update(TapUpdate),
    ManagedBotLifecycle(ManagedBotLifecycle),
}

impl TapEvent {
    fn parent_bot_id(&self) -> i64 {
        match self {
            Self::Update(update) => update.telegram_bot_id,
            Self::ManagedBotLifecycle(event) => event.parent_telegram_bot_id,
        }
    }

    fn lifecycle_ack(&self) -> Option<LifecycleAck> {
        match self {
            Self::Update(_) => None,
            Self::ManagedBotLifecycle(event) => Some(LifecycleAck {
                producer_instance_id: event.producer_instance_id,
                event_sequence: event.event_sequence,
                test_dc: event.test_dc,
                parent_telegram_bot_id: event.parent_telegram_bot_id,
                observer_event_id: event.observer_event_id,
                expires_at: event.expires_at,
                delivery_nonce: event.delivery_nonce,
                managed_owner_telegram_user_id: event.managed_owner_telegram_user_id,
                managed_telegram_bot_id: event.managed_telegram_bot_id,
            }),
        }
    }

    fn expires_at(&self) -> u32 {
        match self {
            Self::Update(update) => update.expires_at,
            Self::ManagedBotLifecycle(event) => event.expires_at,
        }
    }
}

#[derive(Debug)]
struct TapUpdate {
    test_dc: bool,
    telegram_bot_id: i64,
    update_id: i64,
    expires_at: u32,
    payload: Value,
}

#[derive(Debug)]
struct ManagedBotLifecycle {
    producer_instance_id: u64,
    event_sequence: u64,
    test_dc: bool,
    parent_telegram_bot_id: i64,
    observer_event_id: i64,
    expires_at: u32,
    delivery_nonce: u64,
    managed_owner_telegram_user_id: i64,
    managed_telegram_bot_id: i64,
}

#[derive(Clone, Copy, Debug)]
struct LifecycleAck {
    producer_instance_id: u64,
    event_sequence: u64,
    test_dc: bool,
    parent_telegram_bot_id: i64,
    observer_event_id: i64,
    expires_at: u32,
    delivery_nonce: u64,
    managed_owner_telegram_user_id: i64,
    managed_telegram_bot_id: i64,
}

impl LifecycleAck {
    fn encode(self) -> [u8; HEADER_BYTES + 16] {
        let mut frame = [0_u8; HEADER_BYTES + 16];
        frame[0..4].copy_from_slice(ACK_MAGIC);
        frame[4] = VERSION;
        frame[5] = MANAGED_BOT_LIFECYCLE_FRAME;
        frame[6] = u8::from(self.test_dc) * FLAG_TEST_DC;
        frame[7] = HEADER_BYTES as u8;
        frame[8..16].copy_from_slice(&self.producer_instance_id.to_be_bytes());
        frame[16..24].copy_from_slice(&self.event_sequence.to_be_bytes());
        frame[24..32].copy_from_slice(&(self.parent_telegram_bot_id as u64).to_be_bytes());
        frame[32..36].copy_from_slice(&(self.observer_event_id as u32).to_be_bytes());
        frame[36..40].copy_from_slice(&self.expires_at.to_be_bytes());
        frame[40..44].copy_from_slice(&16_u32.to_be_bytes());
        frame[48..50].copy_from_slice(&0_u16.to_be_bytes());
        frame[50..52].copy_from_slice(&1_u16.to_be_bytes());
        frame[52..56].copy_from_slice(&16_u32.to_be_bytes());
        frame[56..64].copy_from_slice(&self.delivery_nonce.to_be_bytes());
        frame[64..72].copy_from_slice(&(self.managed_owner_telegram_user_id as u64).to_be_bytes());
        frame[72..80].copy_from_slice(&(self.managed_telegram_bot_id as u64).to_be_bytes());
        frame
    }
}

#[derive(Default)]
struct Metrics {
    datagrams: AtomicU64,
    malformed_frames: AtomicU64,
    events_reassembled: AtomicU64,
    incomplete_events: AtomicU64,
    capacity_drops: AtomicU64,
    expired_events: AtomicU64,
    queue_drops: AtomicU64,
    unknown_bots: AtomicU64,
    database_errors: AtomicU64,
    invalid_updates: AtomicU64,
    updates_stored: AtomicU64,
    duplicate_updates: AtomicU64,
    managed_lifecycle_queued: AtomicU64,
    managed_lifecycle_duplicates: AtomicU64,
    managed_lifecycle_acks_sent: AtomicU64,
    managed_lifecycle_ack_errors: AtomicU64,
}

impl Metrics {
    fn increment(counter: &AtomicU64, amount: u64) {
        counter.fetch_add(amount, Ordering::Relaxed);
    }

    fn log(&self) {
        tracing::info!(
            datagrams = self.datagrams.load(Ordering::Relaxed),
            malformed_frames = self.malformed_frames.load(Ordering::Relaxed),
            events_reassembled = self.events_reassembled.load(Ordering::Relaxed),
            incomplete_events = self.incomplete_events.load(Ordering::Relaxed),
            capacity_drops = self.capacity_drops.load(Ordering::Relaxed),
            expired_events = self.expired_events.load(Ordering::Relaxed),
            queue_drops = self.queue_drops.load(Ordering::Relaxed),
            unknown_bots = self.unknown_bots.load(Ordering::Relaxed),
            database_errors = self.database_errors.load(Ordering::Relaxed),
            invalid_updates = self.invalid_updates.load(Ordering::Relaxed),
            updates_stored = self.updates_stored.load(Ordering::Relaxed),
            duplicate_updates = self.duplicate_updates.load(Ordering::Relaxed),
            managed_lifecycle_queued = self.managed_lifecycle_queued.load(Ordering::Relaxed),
            managed_lifecycle_duplicates =
                self.managed_lifecycle_duplicates.load(Ordering::Relaxed),
            managed_lifecycle_acks_sent = self.managed_lifecycle_acks_sent.load(Ordering::Relaxed),
            managed_lifecycle_ack_errors =
                self.managed_lifecycle_ack_errors.load(Ordering::Relaxed),
            "tap collector metrics"
        );
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct OfficialCriticalStats {
    values: [u64; OFFICIAL_STATS_NAMES.len()],
}

impl OfficialCriticalStats {
    fn parse(body: &[u8]) -> std::result::Result<Self, String> {
        if body.len() > MAX_OFFICIAL_STATS_BYTES {
            return Err("official statistics response exceeds the size limit".into());
        }
        let body = std::str::from_utf8(body)
            .map_err(|_| "official statistics response is not UTF-8".to_string())?;
        let mut values = [None; OFFICIAL_STATS_NAMES.len()];
        for line in body.lines() {
            let Some((name, value)) = line.split_once('\t') else {
                continue;
            };
            let Some(index) = OFFICIAL_STATS_NAMES
                .iter()
                .position(|expected| *expected == name)
            else {
                continue;
            };
            if values[index].is_some() {
                return Err("official statistics contain a duplicate critical counter".into());
            }
            values[index] = Some(
                value
                    .parse::<u64>()
                    .map_err(|_| "official statistics contain an invalid critical counter")?,
            );
        }
        let mut parsed = [0; OFFICIAL_STATS_NAMES.len()];
        for (index, value) in values.into_iter().enumerate() {
            parsed[index] =
                value.ok_or_else(|| "official statistics omit a critical counter".to_string())?;
        }
        Ok(Self { values: parsed })
    }
}

#[derive(Clone)]
struct OfficialControlState {
    client: reqwest::Client,
    stats_url: Url,
    token_digest: [u8; 32],
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OfficialDrainRequest {
    schema_version: u32,
    bot_token: String,
    telegram_test_dc: bool,
    route_generation: String,
}

#[derive(Serialize)]
struct OfficialDrainResponse {
    schema_version: u32,
    fenced: bool,
    telegram_bot_id: String,
    telegram_test_dc: bool,
    route_generation: String,
    active_requests: String,
}

#[derive(Serialize)]
struct OfficialDrainForm<'a> {
    phenogram_action: &'static str,
    phenogram_bot_token: &'a str,
    phenogram_test_dc: &'static str,
    phenogram_route_generation: &'a str,
}

#[derive(Debug, Eq, PartialEq)]
struct ParsedOfficialDrainProof {
    fenced: bool,
    telegram_bot_id: u64,
    telegram_test_dc: bool,
    route_generation: u64,
    active_requests: u64,
}

impl ParsedOfficialDrainProof {
    fn parse(body: &[u8]) -> std::result::Result<Self, String> {
        if body.len() > MAX_OFFICIAL_CONTROL_RESPONSE_BYTES {
            return Err("official drain response exceeds the size limit".into());
        }
        let body = std::str::from_utf8(body)
            .map_err(|_| "official drain response is not UTF-8".to_string())?;
        let field = |name: &str| -> std::result::Result<&str, String> {
            let mut values = body.lines().filter_map(|line| {
                let (candidate, value) = line.split_once('\t')?;
                (candidate == name).then_some(value)
            });
            let value = values
                .next()
                .ok_or_else(|| "official drain response omits a required field".to_string())?;
            if values.next().is_some() {
                return Err("official drain response duplicates a required field".into());
            }
            Ok(value)
        };
        let parse_u64 = |name: &str| {
            field(name)?
                .parse::<u64>()
                .map_err(|_| "official drain response contains an invalid integer".to_string())
        };
        let parse_bool = |name: &str| -> std::result::Result<bool, String> {
            match field(name)? {
                "0" => Ok(false),
                "1" => Ok(true),
                _ => Err("official drain response contains an invalid boolean".into()),
            }
        };
        Ok(Self {
            fenced: parse_bool("phenogram_drain_fenced")?,
            telegram_bot_id: parse_u64("telegram_bot_id")?,
            telegram_test_dc: parse_bool("telegram_test_dc")?,
            route_generation: parse_u64("route_generation")?,
            active_requests: parse_u64("active_requests")?,
        })
    }
}

async fn official_drain_control(
    State(state): State<OfficialControlState>,
    request: Request,
) -> Response {
    if !valid_control_authorization(request.headers(), &state.token_digest) {
        return official_control_error(StatusCode::UNAUTHORIZED, "unauthorized");
    }
    let body =
        match axum::body::to_bytes(request.into_body(), MAX_OFFICIAL_CONTROL_REQUEST_BYTES).await {
            Ok(body) => body,
            Err(_) => {
                return official_control_error(StatusCode::PAYLOAD_TOO_LARGE, "request_too_large");
            }
        };
    let request: OfficialDrainRequest = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(_) => return official_control_error(StatusCode::BAD_REQUEST, "invalid_request"),
    };
    let Some(telegram_bot_id) = telegram_bot_id(&request.bot_token) else {
        return official_control_error(StatusCode::BAD_REQUEST, "invalid_request");
    };
    let Ok(route_generation) = request.route_generation.parse::<u64>() else {
        return official_control_error(StatusCode::BAD_REQUEST, "invalid_request");
    };
    if request.schema_version != VERSION as u32 || route_generation == 0 {
        return official_control_error(StatusCode::BAD_REQUEST, "invalid_request");
    }
    let form = OfficialDrainForm {
        phenogram_action: "drain",
        phenogram_bot_token: &request.bot_token,
        phenogram_test_dc: if request.telegram_test_dc { "1" } else { "0" },
        phenogram_route_generation: &request.route_generation,
    };
    let form = match serde_urlencoded::to_string(&form) {
        Ok(form) => form,
        Err(_) => return official_control_error(StatusCode::BAD_GATEWAY, "official_unavailable"),
    };
    let response = match state
        .client
        .post(state.stats_url)
        .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
        .body(form)
        .send()
        .await
    {
        Ok(response) => response,
        Err(_) => return official_control_error(StatusCode::BAD_GATEWAY, "official_unavailable"),
    };
    let body = match bounded_response_body(response, MAX_OFFICIAL_CONTROL_RESPONSE_BYTES).await {
        Ok(body) => body,
        Err(_) => return official_control_error(StatusCode::BAD_GATEWAY, "official_unavailable"),
    };
    let proof = match ParsedOfficialDrainProof::parse(&body) {
        Ok(proof)
            if proof.fenced
                && proof.telegram_bot_id == telegram_bot_id
                && proof.telegram_test_dc == request.telegram_test_dc
                && proof.route_generation == route_generation =>
        {
            proof
        }
        _ => return official_control_error(StatusCode::BAD_GATEWAY, "invalid_official_proof"),
    };
    (
        StatusCode::OK,
        [(header::CACHE_CONTROL, "no-store")],
        axum::Json(OfficialDrainResponse {
            schema_version: VERSION as u32,
            fenced: true,
            telegram_bot_id: telegram_bot_id.to_string(),
            telegram_test_dc: request.telegram_test_dc,
            route_generation: route_generation.to_string(),
            active_requests: proof.active_requests.to_string(),
        }),
    )
        .into_response()
}

fn official_control_error(status: StatusCode, code: &'static str) -> Response {
    (
        status,
        [(header::CACHE_CONTROL, "no-store")],
        axum::Json(serde_json::json!({ "error": { "code": code } })),
    )
        .into_response()
}

fn valid_control_authorization(headers: &http::HeaderMap, expected: &[u8; 32]) -> bool {
    let Some(value) = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
    else {
        return false;
    };
    let actual: [u8; 32] = Sha256::digest(value.as_bytes()).into();
    actual.ct_eq(expected).into()
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

fn durability_signal_increase(value: u64, previous: u64) -> Option<u64> {
    (value > 0).then(|| value.saturating_sub(previous))
}

async fn monitor_official_stats(url: Url, interval: Duration, tap_pool: TapPool) {
    let client = match official_http_client() {
        Ok(client) => client,
        Err(_) => {
            tracing::error!(
                pool = tap_pool.as_str(),
                "official Bot API statistics monitor could not initialize"
            );
            return;
        }
    };
    let mut previous = OfficialCriticalStats {
        values: [0; OFFICIAL_STATS_NAMES.len()],
    };
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        ticker.tick().await;
        match fetch_official_stats(&client, &url).await {
            Ok(current) => {
                for (index, counter) in OFFICIAL_STATS_NAMES.iter().enumerate() {
                    let value = current.values[index];
                    if let Some(increase) =
                        durability_signal_increase(value, previous.values[index])
                    {
                        tracing::error!(
                            signal = "managed_lifecycle_durability",
                            pool = tap_pool.as_str(),
                            counter = *counter,
                            value,
                            increase,
                            "official Bot API lifecycle durability counter is non-zero"
                        );
                    }
                }
                previous = current;
            }
            Err(error) => tracing::warn!(
                pool = tap_pool.as_str(),
                reason = error,
                "official Bot API statistics poll failed; collector remains available"
            ),
        }
    }
}

fn official_http_client() -> std::result::Result<reqwest::Client, String> {
    reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(1))
        .timeout(Duration::from_secs(2))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|_| "official HTTP client could not initialize".to_string())
}

async fn fetch_official_stats(
    client: &reqwest::Client,
    url: &Url,
) -> std::result::Result<OfficialCriticalStats, String> {
    let response = client
        .get(url.clone())
        .send()
        .await
        .map_err(|_| "official statistics endpoint is unavailable".to_string())?;
    let body = bounded_response_body(response, MAX_OFFICIAL_STATS_BYTES).await?;
    OfficialCriticalStats::parse(&body)
}

async fn bounded_response_body(
    response: reqwest::Response,
    maximum: usize,
) -> std::result::Result<Vec<u8>, String> {
    if response.status() != reqwest::StatusCode::OK {
        return Err("official endpoint returned a non-success status".into());
    }
    if response
        .content_length()
        .is_some_and(|length| length > maximum as u64)
    {
        return Err("official response exceeds the size limit".into());
    }
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| "official response body failed".to_string())?;
        if body
            .len()
            .checked_add(chunk.len())
            .is_none_or(|length| length > maximum)
        {
            return Err("official response exceeds the size limit".into());
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

pub async fn run(config: TapConfig) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let pool = PgPoolOptions::new()
        .max_connections(DATABASE_WORKERS as u32)
        .min_connections(0)
        .acquire_timeout(Duration::from_secs(1))
        .connect_lazy(&config.database_url)?;
    let socket = bind_socket(&config.socket_path)?;
    let ack_socket = Arc::new(UnixDatagram::unbound()?);
    let _socket_guard = SocketGuard::new(&config.socket_path)?;
    let metrics = Arc::new(Metrics::default());
    let worker_count = DATABASE_WORKERS.min(config.max_inflight_events);
    let mut senders = Vec::with_capacity(worker_count);
    let mut worker_handles = Vec::with_capacity(worker_count);
    for worker_index in 0..worker_count {
        let capacity = config.max_inflight_events / worker_count
            + usize::from(worker_index < config.max_inflight_events % worker_count);
        let (sender, receiver) = mpsc::channel(capacity);
        senders.push(sender);
        worker_handles.push(tokio::spawn(database_worker(
            pool.clone(),
            config.pool,
            ack_socket.clone(),
            config.ack_socket_path.clone(),
            receiver,
            metrics.clone(),
        )));
    }
    let official_stats_handle = config.official_stats_url.clone().map(|url| {
        tokio::spawn(monitor_official_stats(
            url,
            config.official_stats_interval,
            config.pool,
        ))
    });
    let official_control_handle = match (
        config.official_control_listen_addr,
        config.official_control_token_digest,
        config.official_stats_url.clone(),
    ) {
        (Some(listen_addr), Some(token_digest), Some(stats_url)) => {
            let listener = tokio::net::TcpListener::bind(listen_addr).await?;
            let state = OfficialControlState {
                client: official_http_client().map_err(std::io::Error::other)?,
                stats_url,
                token_digest,
            };
            let router = Router::new()
                .route("/internal/official/drain", post(official_drain_control))
                .with_state(state);
            Some(tokio::spawn(async move {
                if let Err(error) = axum::serve(listener, router).await {
                    tracing::error!(reason = %error, "official drain helper stopped");
                }
            }))
        }
        (None, None, _) => None,
        _ => {
            return Err(
                std::io::Error::other("tap control listener configuration is incomplete").into(),
            );
        }
    };
    tracing::info!(
        socket = %config.socket_path.display(),
        ack_socket = %config.ack_socket_path.display(),
        pool = config.pool.as_str(),
        max_event_bytes = config.max_event_bytes,
        max_inflight_events = config.max_inflight_events,
        max_inflight_bytes = config.max_inflight_bytes,
        reassembly_timeout_ms = config.reassembly_timeout.as_millis(),
        "tap collector ready"
    );

    let mut reassembler = Reassembler::new(&config);
    let mut datagram = vec![0_u8; MAX_DATAGRAM_BYTES + 1];
    let cleanup_period = config
        .reassembly_timeout
        .checked_div(2)
        .unwrap_or(Duration::from_millis(100))
        .clamp(Duration::from_millis(100), Duration::from_secs(1));
    let mut cleanup = tokio::time::interval(cleanup_period);
    cleanup.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut metric_report = tokio::time::interval(Duration::from_secs(30));
    metric_report.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    metric_report.tick().await;

    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            received = socket.recv(&mut datagram) => {
                let received = received?;
                Metrics::increment(&metrics.datagrams, 1);
                let completed = match reassembler.accept(&datagram[..received], Instant::now()) {
                    Ok(completed) => completed,
                    Err(error) => {
                        if matches!(error, FrameError::Capacity) {
                            Metrics::increment(&metrics.capacity_drops, 1);
                        } else {
                            Metrics::increment(&metrics.malformed_frames, 1);
                        }
                        tracing::warn!(error = %error, datagram_bytes = received, "dropped tap frame");
                        None
                    }
                };
                let Some(completed) = completed else { continue };
                Metrics::increment(&metrics.events_reassembled, 1);
                let event = match decode_event(completed) {
                    Ok(event) => event,
                    Err(error) => {
                        Metrics::increment(&metrics.invalid_updates, 1);
                        tracing::warn!(error = %error, "dropped malformed reassembled update");
                        continue;
                    }
                };
                if event.expires_at() as u64 <= unix_timestamp() {
                    Metrics::increment(&metrics.expired_events, 1);
                    continue;
                }
                let shard = event.parent_bot_id().unsigned_abs() as usize % senders.len();
                if senders[shard].try_send(event).is_err() {
                    Metrics::increment(&metrics.queue_drops, 1);
                }
            }
            _ = cleanup.tick() => {
                let expired = reassembler.purge_expired(Instant::now());
                if expired > 0 {
                    Metrics::increment(&metrics.incomplete_events, expired as u64);
                    tracing::warn!(expired, "dropped incomplete tap events after reassembly timeout");
                }
            }
            _ = metric_report.tick() => metrics.log(),
            _ = &mut shutdown => break,
        }
    }

    drop(senders);
    for handle in worker_handles {
        handle.abort();
    }
    if let Some(handle) = official_stats_handle {
        handle.abort();
    }
    if let Some(handle) = official_control_handle {
        handle.abort();
    }
    metrics.log();
    Ok(())
}

async fn database_worker(
    pool: PgPool,
    tap_pool: TapPool,
    ack_socket: Arc<UnixDatagram>,
    ack_socket_path: PathBuf,
    mut receiver: mpsc::Receiver<TapEvent>,
    metrics: Arc<Metrics>,
) {
    while let Some(event) = receiver.recv().await {
        if event.expires_at() as u64 <= unix_timestamp() {
            Metrics::increment(&metrics.expired_events, 1);
            continue;
        }
        let lifecycle_ack = event.lifecycle_ack();
        match tokio::time::timeout(
            DATABASE_OPERATION_TIMEOUT,
            persist_event(&pool, tap_pool, event),
        )
        .await
        {
            Ok(Ok(PersistOutcome::Stored)) => Metrics::increment(&metrics.updates_stored, 1),
            Ok(Ok(PersistOutcome::Duplicate)) => Metrics::increment(&metrics.duplicate_updates, 1),
            Ok(Ok(PersistOutcome::UnknownBot)) => Metrics::increment(&metrics.unknown_bots, 1),
            Ok(Ok(PersistOutcome::ManagedLifecycleQueued)) => {
                Metrics::increment(&metrics.managed_lifecycle_queued, 1);
                send_lifecycle_ack(&ack_socket, &ack_socket_path, lifecycle_ack, &metrics);
            }
            Ok(Ok(PersistOutcome::ManagedLifecycleDuplicate)) => {
                Metrics::increment(&metrics.managed_lifecycle_duplicates, 1);
                send_lifecycle_ack(&ack_socket, &ack_socket_path, lifecycle_ack, &metrics);
            }
            Ok(Err(AppError::Validation(error))) => {
                Metrics::increment(&metrics.invalid_updates, 1);
                tracing::warn!(error, "dropped invalid tap update");
            }
            Ok(Err(error)) => {
                Metrics::increment(&metrics.database_errors, 1);
                tracing::warn!(error = ?error, "tap observer database operation failed; update dropped");
            }
            Err(_) => {
                Metrics::increment(&metrics.database_errors, 1);
                tracing::warn!("tap observer database operation timed out; update dropped");
            }
        }
    }
}

fn send_lifecycle_ack(
    socket: &UnixDatagram,
    path: &Path,
    ack: Option<LifecycleAck>,
    metrics: &Metrics,
) {
    let Some(ack) = ack else {
        Metrics::increment(&metrics.managed_lifecycle_ack_errors, 1);
        return;
    };
    match socket.try_send_to(&ack.encode(), path) {
        Ok(bytes) if bytes == HEADER_BYTES + 16 => {
            Metrics::increment(&metrics.managed_lifecycle_acks_sent, 1);
        }
        Ok(_) | Err(_) => {
            Metrics::increment(&metrics.managed_lifecycle_ack_errors, 1);
        }
    }
}

enum PersistOutcome {
    Stored,
    Duplicate,
    UnknownBot,
    ManagedLifecycleQueued,
    ManagedLifecycleDuplicate,
}

async fn persist_event(
    pool: &PgPool,
    tap_pool: TapPool,
    event: TapEvent,
) -> Result<PersistOutcome> {
    match event {
        TapEvent::Update(update) => persist_update(pool, tap_pool, update).await,
        TapEvent::ManagedBotLifecycle(event) => {
            persist_managed_lifecycle(pool, tap_pool, event).await
        }
    }
}

async fn resolve_ingestion_bot(
    pool: &PgPool,
    tap_pool: TapPool,
    telegram_bot_id: i64,
    telegram_test_dc: bool,
) -> Result<Option<IngestionBot>> {
    let bot = sqlx::query_as::<_, (Uuid, i64)>(
        r#"SELECT id, telegram_bot_id
             FROM bots
            WHERE telegram_bot_id = $1
              AND telegram_test_dc = $3
              AND (
                    $2 = data_plane_pool
                    OR $2 = data_plane_target_pool
                    OR EXISTS (
                        SELECT 1
                          FROM bot_data_plane_operations operation
                         WHERE operation.bot_id = bots.id
                           AND operation.source_pool = $2
                           AND operation.phase IN (
                               'route_withdrawn',
                               'webhook_captured',
                               'webhook_deleted',
                               'logout_started',
                               'close_started',
                               'manual_recovery'
                           )
                    )
              )"#,
    )
    .bind(telegram_bot_id)
    .bind(tap_pool.as_str())
    .bind(telegram_test_dc)
    .fetch_optional(pool)
    .await?;
    Ok(bot.map(|(id, telegram_bot_id)| IngestionBot {
        id,
        telegram_bot_id,
    }))
}

async fn persist_update(
    pool: &PgPool,
    tap_pool: TapPool,
    update: TapUpdate,
) -> Result<PersistOutcome> {
    let Some(bot) =
        resolve_ingestion_bot(pool, tap_pool, update.telegram_bot_id, update.test_dc).await?
    else {
        return Ok(PersistOutcome::UnknownBot);
    };
    let outcome = ingest_update(
        pool,
        bot,
        update.payload,
        IngestionSource::OfficialTap,
        Some(update.update_id),
    )
    .await?;
    Ok(match outcome {
        IngestionOutcome::Inserted(_) => PersistOutcome::Stored,
        IngestionOutcome::Duplicate => PersistOutcome::Duplicate,
    })
}

async fn persist_managed_lifecycle(
    pool: &PgPool,
    tap_pool: TapPool,
    event: ManagedBotLifecycle,
) -> Result<PersistOutcome> {
    let Some(parent) =
        resolve_ingestion_bot(pool, tap_pool, event.parent_telegram_bot_id, event.test_dc).await?
    else {
        return Ok(PersistOutcome::UnknownBot);
    };
    Ok(
        match ingest_managed_bot_lifecycle(
            pool,
            parent,
            ManagedLifecycleDelivery {
                data_plane_pool: tap_pool.as_str(),
                telegram_test_dc: event.test_dc,
                observer_event_id: event.observer_event_id,
                delivery_nonce: i64::try_from(event.delivery_nonce).map_err(|_| {
                    AppError::Validation("invalid managed lifecycle delivery nonce".into())
                })?,
                expires_at: event.expires_at,
            },
            event.managed_owner_telegram_user_id,
            event.managed_telegram_bot_id,
        )
        .await?
        {
            ManagedLifecycleOutcome::Queued => PersistOutcome::ManagedLifecycleQueued,
            ManagedLifecycleOutcome::Duplicate => PersistOutcome::ManagedLifecycleDuplicate,
        },
    )
}

fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn bind_socket(path: &Path) -> std::io::Result<UnixDatagram> {
    let parent = path.parent().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "socket has no parent")
    })?;
    fs::create_dir_all(parent)?;
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_socket() => fs::remove_file(path)?,
        Ok(_) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "refusing to replace a non-socket tap path",
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let socket = UnixDatagram::bind(path)?;
    // A maximum-size event is nine back-to-back datagrams. Request enough
    // kernel queue space for several such events so an otherwise healthy
    // collector does not lose the tail solely because of a small default.
    // The producer remains non-blocking and receives no acknowledgement.
    socket2::SockRef::from(&socket).set_recv_buffer_size(SOCKET_RECEIVE_BUFFER_BYTES)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o660))?;
    Ok(socket)
}

struct SocketGuard {
    path: PathBuf,
    device: u64,
    inode: u64,
}

impl SocketGuard {
    fn new(path: &Path) -> std::io::Result<Self> {
        let metadata = fs::symlink_metadata(path)?;
        Ok(Self {
            path: path.to_owned(),
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }
}

impl Drop for SocketGuard {
    fn drop(&mut self) {
        if let Ok(metadata) = fs::symlink_metadata(&self.path)
            && metadata.file_type().is_socket()
            && metadata.dev() == self.device
            && metadata.ino() == self.inode
        {
            let _ = fs::remove_file(&self.path);
        }
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler")
    };
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };
    tokio::select! { _ = ctrl_c => {}, _ = terminate => {} }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use axum::http::header;
    use sha2::{Digest, Sha256};

    use super::{
        EventKey, FLAG_TEST_DC, FrameError, HEADER_BYTES, OfficialCriticalStats,
        ParsedOfficialDrainProof, Reassembler, TapConfig, TapEvent, TapPool, decode_event,
        durability_signal_increase, parse_official_stats_url, telegram_bot_id,
        valid_control_authorization,
    };

    fn config(max_events: usize, max_bytes: usize) -> TapConfig {
        TapConfig {
            database_url: "postgres://unused".into(),
            pool: TapPool::Standard,
            socket_path: "/tmp/unused-tap.sock".into(),
            ack_socket_path: "/tmp/unused-tap-ack.sock".into(),
            max_event_bytes: 262_144,
            max_inflight_events: max_events,
            max_inflight_bytes: max_bytes,
            reassembly_timeout: Duration::from_millis(100),
            official_stats_url: None,
            official_stats_interval: Duration::from_secs(15),
            official_control_listen_addr: None,
            official_control_token_digest: None,
        }
    }

    #[test]
    fn tap_pool_config_requires_an_exact_explicit_pool() {
        assert_eq!(
            TapPool::from_config_value(Some("standard")),
            Ok(TapPool::Standard)
        );
        assert_eq!(
            TapPool::from_config_value(Some("local")),
            Ok(TapPool::Local)
        );
        for invalid in [
            None,
            Some(""),
            Some("cloud"),
            Some("Standard"),
            Some(" local"),
        ] {
            assert!(TapPool::from_config_value(invalid).is_err());
        }
    }

    #[test]
    fn official_stats_monitor_accepts_only_the_private_loopback_endpoint() {
        assert_eq!(parse_official_stats_url(None), Ok(None));
        assert_eq!(
            parse_official_stats_url(Some("http://127.0.0.1:8083/".into()))
                .expect("loopback statistics URL")
                .expect("configured URL")
                .as_str(),
            "http://127.0.0.1:8083/"
        );
        for invalid in [
            "http://0.0.0.0:8083/",
            "http://127.0.0.1:8084/",
            "https://127.0.0.1:8083/",
            "http://127.0.0.1:8083/stats",
            "http://user@127.0.0.1:8083/",
        ] {
            assert!(parse_official_stats_url(Some(invalid.into())).is_err());
        }
    }

    #[test]
    fn parses_all_official_lifecycle_durability_counters_strictly() {
        let parsed = OfficialCriticalStats::parse(
            b"uptime\t1\nmanaged_lifecycle_overflow\t2\nmanaged_lifecycle_persistence_errors\t3\nmanaged_lifecycle_expired\t4\nmanaged_lifecycle_ack_errors\t5\n",
        )
        .expect("official critical counters");
        assert_eq!(parsed.values, [2, 3, 4, 5]);
        assert!(
            OfficialCriticalStats::parse(
                b"managed_lifecycle_overflow\t0\nmanaged_lifecycle_persistence_errors\t0\nmanaged_lifecycle_expired\t0\n"
            )
            .is_err()
        );
        assert!(
            OfficialCriticalStats::parse(
                b"managed_lifecycle_overflow\t0\nmanaged_lifecycle_overflow\t1\nmanaged_lifecycle_persistence_errors\t0\nmanaged_lifecycle_expired\t0\nmanaged_lifecycle_ack_errors\t0\n"
            )
            .is_err()
        );
        assert_eq!(durability_signal_increase(0, 0), None);
        assert_eq!(durability_signal_increase(2, 0), Some(2));
        assert_eq!(durability_signal_increase(2, 2), Some(0));
        assert_eq!(durability_signal_increase(5, 2), Some(3));
    }

    #[test]
    fn validates_token_free_official_drain_proofs_and_control_auth() {
        let proof = ParsedOfficialDrainProof::parse(
            b"phenogram_drain_fenced\t1\ntelegram_bot_id\t123\ntelegram_test_dc\t0\nroute_generation\t42\nactive_requests\t1\n",
        )
        .expect("official drain proof");
        assert_eq!(proof.telegram_bot_id, 123);
        assert_eq!(proof.route_generation, 42);
        assert_eq!(proof.active_requests, 1);
        assert!(proof.fenced);
        assert!(!proof.telegram_test_dc);
        assert!(ParsedOfficialDrainProof::parse(b"phenogram_drain_fenced\t1\n").is_err());
        assert_eq!(telegram_bot_id("123:secret"), Some(123));
        assert_eq!(telegram_bot_id("0123:secret"), None);

        let token = "s".repeat(32);
        let digest: [u8; 32] = Sha256::digest(token.as_bytes()).into();
        let mut headers = http::HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            format!("Bearer {token}").parse().expect("authorization"),
        );
        assert!(valid_control_authorization(&headers, &digest));
        headers.insert(
            header::AUTHORIZATION,
            "Bearer wrong".parse().expect("wrong authorization"),
        );
        assert!(!valid_control_authorization(&headers, &digest));
    }

    #[allow(clippy::too_many_arguments)]
    fn frame(
        producer: u64,
        sequence: u64,
        bot_id: u64,
        update_id: u32,
        expiry: u32,
        total: usize,
        index: u16,
        count: u16,
        offset: usize,
        payload: &[u8],
    ) -> Vec<u8> {
        let mut value = vec![0_u8; HEADER_BYTES + payload.len()];
        value[0..4].copy_from_slice(b"PGUT");
        value[4] = 1;
        value[5] = 1;
        value[7] = HEADER_BYTES as u8;
        value[8..16].copy_from_slice(&producer.to_be_bytes());
        value[16..24].copy_from_slice(&sequence.to_be_bytes());
        value[24..32].copy_from_slice(&bot_id.to_be_bytes());
        value[32..36].copy_from_slice(&update_id.to_be_bytes());
        value[36..40].copy_from_slice(&expiry.to_be_bytes());
        value[40..44].copy_from_slice(&(total as u32).to_be_bytes());
        value[44..48].copy_from_slice(&(offset as u32).to_be_bytes());
        value[48..50].copy_from_slice(&index.to_be_bytes());
        value[50..52].copy_from_slice(&count.to_be_bytes());
        value[52..56].copy_from_slice(&(payload.len() as u32).to_be_bytes());
        value[HEADER_BYTES..].copy_from_slice(payload);
        value
    }

    #[test]
    fn reassembles_out_of_order_fragments_into_canonical_update() {
        let member = br#""message":{"text":"hello"}"#;
        let split = 9;
        let now = Instant::now();
        let mut reassembler = Reassembler::new(&config(4, 1024));
        assert!(
            reassembler
                .accept(
                    &frame(
                        1,
                        2,
                        99,
                        7001,
                        u32::MAX,
                        member.len(),
                        1,
                        2,
                        split,
                        &member[split..]
                    ),
                    now,
                )
                .expect("second fragment")
                .is_none()
        );
        let completed = reassembler
            .accept(
                &frame(
                    1,
                    2,
                    99,
                    7001,
                    u32::MAX,
                    member.len(),
                    0,
                    2,
                    0,
                    &member[..split],
                ),
                now,
            )
            .expect("first fragment")
            .expect("complete event");
        let TapEvent::Update(update) = decode_event(completed).expect("canonical JSON") else {
            panic!("expected canonical update")
        };
        assert_eq!(update.telegram_bot_id, 99);
        assert_eq!(update.update_id, 7001);
        assert_eq!(update.payload["update_id"], 7001);
        assert_eq!(update.payload["message"]["text"], "hello");
        assert_eq!(reassembler.reserved_bytes, 0);
    }

    #[test]
    fn reassembles_the_protocol_maximum_nine_datagram_event() {
        let total = super::PROTOCOL_MAX_EVENT_BYTES;
        let payload = vec![b'x'; total];
        let chunks = payload
            .chunks(super::MAX_FRAGMENT_BYTES)
            .collect::<Vec<_>>();
        assert_eq!(chunks.len(), super::PROTOCOL_MAX_FRAGMENTS);
        let now = Instant::now();
        let mut reassembler = Reassembler::new(&config(1, total));
        let mut completed = None;
        for (index, chunk) in chunks.into_iter().enumerate() {
            completed = reassembler
                .accept(
                    &frame(
                        1,
                        2,
                        99,
                        7001,
                        u32::MAX,
                        total,
                        index as u16,
                        super::PROTOCOL_MAX_FRAGMENTS as u16,
                        index * super::MAX_FRAGMENT_BYTES,
                        chunk,
                    ),
                    now,
                )
                .expect("valid maximum event fragment");
        }
        let completed = completed.expect("maximum event must complete");
        assert_eq!(completed.member_json, payload);
        assert_eq!(reassembler.reserved_bytes, 0);
    }

    #[test]
    fn duplicate_fragment_is_idempotent_but_conflicting_duplicate_drops_event() {
        let now = Instant::now();
        let mut reassembler = Reassembler::new(&config(2, 1024));
        let first = frame(1, 2, 99, 7, u32::MAX, 2, 0, 2, 0, b"a");
        assert!(reassembler.accept(&first, now).unwrap().is_none());
        assert!(reassembler.accept(&first, now).unwrap().is_none());
        let conflicting = frame(1, 2, 99, 7, u32::MAX, 2, 0, 2, 0, b"b");
        assert!(matches!(
            reassembler.accept(&conflicting, now),
            Err(FrameError::ConflictingDuplicate)
        ));
        assert!(reassembler.events.is_empty());
        assert_eq!(reassembler.reserved_bytes, 0);
    }

    #[test]
    fn changed_metadata_and_gapped_coverage_drop_whole_event() {
        let now = Instant::now();
        let mut reassembler = Reassembler::new(&config(2, 1024));
        reassembler
            .accept(&frame(1, 2, 99, 7, u32::MAX, 2, 0, 2, 0, b"a"), now)
            .unwrap();
        assert!(matches!(
            reassembler.accept(&frame(1, 2, 100, 7, u32::MAX, 2, 1, 2, 1, b"b"), now),
            Err(FrameError::MetadataChanged)
        ));

        reassembler
            .accept(&frame(3, 4, 99, 8, u32::MAX, 3, 0, 2, 0, b"a"), now)
            .unwrap();
        assert!(matches!(
            reassembler.accept(&frame(3, 4, 99, 8, u32::MAX, 3, 1, 2, 2, b"b"), now),
            Err(FrameError::Coverage)
        ));
        assert_eq!(reassembler.reserved_bytes, 0);
    }

    #[test]
    fn capacity_is_reserved_by_declared_size_and_released_on_timeout() {
        let now = Instant::now();
        let mut reassembler = Reassembler::new(&config(1, 4));
        reassembler
            .accept(&frame(1, 1, 99, 1, u32::MAX, 4, 0, 2, 0, b"a"), now)
            .unwrap();
        assert!(matches!(
            reassembler.accept(&frame(1, 2, 99, 2, u32::MAX, 1, 0, 1, 0, b"x"), now),
            Err(FrameError::Capacity)
        ));
        assert_eq!(
            reassembler.purge_expired(now + Duration::from_millis(100)),
            1
        );
        assert_eq!(reassembler.reserved_bytes, 0);
        assert!(reassembler.events.is_empty());
    }

    #[test]
    fn rejects_truncation_oversize_and_nonzero_reserved_bytes() {
        let now = Instant::now();
        let mut reassembler = Reassembler::new(&config(2, 1024));
        let mut truncated = frame(1, 1, 99, 1, u32::MAX, 1, 0, 1, 0, b"x");
        truncated[52..56].copy_from_slice(&2_u32.to_be_bytes());
        assert!(matches!(
            reassembler.accept(&truncated, now),
            Err(FrameError::Length)
        ));
        let oversize = frame(1, 1, 99, 1, u32::MAX, 262_145, 0, 1, 0, b"x");
        assert!(matches!(
            reassembler.accept(&oversize, now),
            Err(FrameError::EventTooLarge)
        ));
        let mut reserved = frame(1, 1, 99, 1, u32::MAX, 1, 0, 1, 0, b"x");
        reserved[63] = 1;
        assert!(matches!(
            reassembler.accept(&reserved, now),
            Err(FrameError::Reserved)
        ));
    }

    #[test]
    fn invalid_member_json_is_rejected_after_complete_reassembly() {
        let now = Instant::now();
        let mut reassembler = Reassembler::new(&config(1, 1024));
        let completed = reassembler
            .accept(&frame(1, 1, 99, 1, u32::MAX, 4, 0, 1, 0, b"nope"), now)
            .unwrap()
            .unwrap();
        assert!(matches!(decode_event(completed), Err(FrameError::Json)));
    }

    #[test]
    fn decodes_durable_managed_bot_lifecycle_frame_and_exact_ack() {
        let now = Instant::now();
        let mut payload = Vec::new();
        payload.extend_from_slice(&555_u64.to_be_bytes());
        payload.extend_from_slice(&987_654_321_u64.to_be_bytes());
        let mut lifecycle = frame(7, 8, 123, 91, u32::MAX, 16, 0, 1, 0, &payload);
        lifecycle[5] = 2;
        lifecycle[6] = FLAG_TEST_DC;
        lifecycle[56..64].copy_from_slice(&777_u64.to_be_bytes());
        let mut reassembler = Reassembler::new(&config(1, 1024));
        let completed = reassembler
            .accept(&lifecycle, now)
            .expect("valid lifecycle frame")
            .expect("single-frame event");
        let TapEvent::ManagedBotLifecycle(event) =
            decode_event(completed).expect("valid lifecycle payload")
        else {
            panic!("expected managed lifecycle")
        };
        assert_eq!(event.parent_telegram_bot_id, 123);
        assert!(event.test_dc);
        assert_eq!(event.observer_event_id, 91);
        assert_eq!(event.expires_at, u32::MAX);
        assert_eq!(event.delivery_nonce, 777);
        assert_eq!(event.managed_owner_telegram_user_id, 555);
        assert_eq!(event.managed_telegram_bot_id, 987_654_321);

        let ack = TapEvent::ManagedBotLifecycle(event)
            .lifecycle_ack()
            .expect("lifecycle ack")
            .encode();
        assert_eq!(&ack[0..4], super::ACK_MAGIC);
        assert_eq!(&ack[4..], &lifecycle[4..]);

        lifecycle[56..64].fill(0);
        assert!(matches!(
            reassembler.accept(&lifecycle, now),
            Err(FrameError::Fragment)
        ));
    }

    #[test]
    fn event_key_includes_producer_restart_identity() {
        assert_ne!(
            EventKey {
                producer_instance_id: 1,
                event_sequence: 9
            },
            EventKey {
                producer_instance_id: 2,
                event_sequence: 9
            }
        );
    }
}
