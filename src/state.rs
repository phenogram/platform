use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex as StdMutex, Weak},
    time::{Duration, Instant},
};

use sqlx::PgPool;
use tokio::sync::{Mutex, OwnedMutexGuard, OwnedSemaphorePermit, RwLock, Semaphore, broadcast};
use uuid::Uuid;

use crate::{config::Config, crypto::Crypto};

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub db: PgPool,
    pub crypto: Arc<Crypto>,
    pub telegram: reqwest::Client,
    pub oauth: reqwest::Client,
    pub events: EventBus,
    pub auth_limiter: AuthLimiter,
    pub stream_limiter: StreamLimiter,
    pub console_stream_limiter: StreamLimiter,
    /// Shared fail-open budget for best-effort audit/timeline/API-call writes.
    /// Telegram request delivery and responses never wait for these permits.
    pub observation_budget: Arc<Semaphore>,
    /// API-call metrics are lower priority than chat timeline persistence and
    /// therefore have an independent budget that cannot starve Bot View.
    pub api_call_observation_budget: Arc<Semaphore>,
    /// Short-lived capabilities for files returned by a successful Bot View
    /// action before the detached timeline write becomes visible.
    pub pending_media: PendingMediaCapabilities,
}

impl AppState {
    pub fn new(config: Config, db: PgPool) -> crate::error::Result<Self> {
        let telegram = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(70))
            .redirect(reqwest::redirect::Policy::none())
            .user_agent(concat!("Phenogram-Platform/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|error| crate::error::AppError::Config(error.to_string()))?;
        let crypto = Crypto::new(
            &config.master_key,
            &config.public_id_key,
            &config.link_signing_key,
        );
        let oauth = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(15))
            .redirect(reqwest::redirect::Policy::none())
            .user_agent(concat!("Phenogram-Platform/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|error| crate::error::AppError::Config(error.to_string()))?;
        Ok(Self {
            config: Arc::new(config),
            db,
            crypto: Arc::new(crypto),
            telegram,
            oauth,
            events: EventBus::default(),
            auth_limiter: AuthLimiter::default(),
            stream_limiter: StreamLimiter::default(),
            console_stream_limiter: StreamLimiter::with_limits(256, 4),
            observation_budget: Arc::new(Semaphore::new(128)),
            api_call_observation_budget: Arc::new(Semaphore::new(32)),
            pending_media: PendingMediaCapabilities::default(),
        })
    }
}

#[derive(Clone, Default)]
pub struct PendingMediaCapabilities {
    entries: Arc<StdMutex<HashMap<(Uuid, String), Instant>>>,
}

impl PendingMediaCapabilities {
    pub fn authorize<'a>(&self, bot_id: Uuid, file_ids: impl IntoIterator<Item = &'a str>) {
        let now = Instant::now();
        let Ok(mut entries) = self.entries.try_lock() else {
            return;
        };
        entries.retain(|_, expires_at| *expires_at > now);
        for file_id in file_ids.into_iter().take(128) {
            if entries.len() >= 2_048 {
                break;
            }
            entries.insert(
                (bot_id, file_id.to_owned()),
                now + Duration::from_secs(5 * 60),
            );
        }
    }

    pub fn contains(&self, bot_id: Uuid, file_id: &str) -> bool {
        let now = Instant::now();
        let Ok(mut entries) = self.entries.try_lock() else {
            return false;
        };
        entries.retain(|_, expires_at| *expires_at > now);
        entries.contains_key(&(bot_id, file_id.to_owned()))
    }
}

#[derive(Clone)]
pub struct StreamLimiter {
    global: Arc<Semaphore>,
    per_key: Arc<StdMutex<HashMap<Vec<u8>, Weak<Semaphore>>>>,
    per_key_limit: usize,
}

pub struct StreamPermit {
    _global: OwnedSemaphorePermit,
    _per_key: OwnedSemaphorePermit,
}

impl Default for StreamLimiter {
    fn default() -> Self {
        Self::with_limits(256, 4)
    }
}

impl StreamLimiter {
    fn with_limits(global_limit: usize, per_key_limit: usize) -> Self {
        Self {
            global: Arc::new(Semaphore::new(global_limit)),
            per_key: Arc::new(StdMutex::new(HashMap::new())),
            per_key_limit,
        }
    }

    pub fn try_acquire(&self, key: &[u8]) -> crate::error::Result<StreamPermit> {
        let global = self
            .global
            .clone()
            .try_acquire_owned()
            .map_err(|_| crate::error::AppError::RateLimited)?;
        let semaphore = {
            let mut per_key = self
                .per_key
                .lock()
                .map_err(|_| crate::error::AppError::Internal)?;
            per_key.retain(|_, semaphore| semaphore.strong_count() > 0);
            if let Some(semaphore) = per_key.get(key).and_then(Weak::upgrade) {
                semaphore
            } else {
                let semaphore = Arc::new(Semaphore::new(self.per_key_limit));
                per_key.insert(key.to_vec(), Arc::downgrade(&semaphore));
                semaphore
            }
        };
        let per_key = semaphore
            .try_acquire_owned()
            .map_err(|_| crate::error::AppError::RateLimited)?;
        Ok(StreamPermit {
            _global: global,
            _per_key: per_key,
        })
    }
}

#[derive(Clone)]
pub struct AuthLimiter {
    attempts: Arc<Mutex<HashMap<String, VecDeque<Instant>>>>,
}

impl Default for AuthLimiter {
    fn default() -> Self {
        Self {
            attempts: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl AuthLimiter {
    pub async fn check(&self, source: &str, identity: &str) -> crate::error::Result<()> {
        const WINDOW: Duration = Duration::from_secs(10 * 60);
        let now = Instant::now();
        let mut attempts = self.attempts.lock().await;
        attempts.retain(|_, entries| {
            while entries
                .front()
                .is_some_and(|time| now.duration_since(*time) > WINDOW)
            {
                entries.pop_front();
            }
            !entries.is_empty()
        });
        for (key, limit) in [
            (format!("source:{source}"), 30_usize),
            (format!("source-identity:{source}:{identity}"), 8_usize),
        ] {
            let entries = attempts.entry(key).or_default();
            if entries.len() >= limit {
                return Err(crate::error::AppError::RateLimited);
            }
            entries.push_back(now);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, serde::Serialize, sqlx::FromRow)]
pub struct StoredUpdate {
    pub row_id: i64,
    pub update_id: i64,
    pub event_type: String,
    pub chat_id: Option<i64>,
    pub telegram_user_id: Option<i64>,
    pub payload: serde_json::Value,
    pub received_at: chrono::DateTime<chrono::Utc>,
    pub expires_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Clone, Default)]
pub struct EventBus {
    senders: Arc<RwLock<HashMap<Uuid, broadcast::Sender<StoredUpdate>>>>,
    ingestion_locks: Arc<RwLock<HashMap<Uuid, Weak<Mutex<()>>>>>,
}

impl EventBus {
    pub async fn lock_ingestion(&self, bot_id: Uuid) -> OwnedMutexGuard<()> {
        let lock = if let Some(lock) = self
            .ingestion_locks
            .read()
            .await
            .get(&bot_id)
            .and_then(Weak::upgrade)
        {
            lock
        } else {
            let mut locks = self.ingestion_locks.write().await;
            locks.retain(|_, lock| lock.strong_count() > 0);
            if let Some(lock) = locks.get(&bot_id).and_then(Weak::upgrade) {
                lock
            } else {
                let lock = Arc::new(Mutex::new(()));
                locks.insert(bot_id, Arc::downgrade(&lock));
                lock
            }
        };
        lock.lock_owned().await
    }

    pub async fn subscribe(&self, bot_id: Uuid) -> broadcast::Receiver<StoredUpdate> {
        self.sender(bot_id).await.subscribe()
    }

    pub async fn publish(&self, bot_id: Uuid, update: StoredUpdate) {
        let _ = self.sender(bot_id).await.send(update);
    }

    async fn sender(&self, bot_id: Uuid) -> broadcast::Sender<StoredUpdate> {
        if let Some(sender) = self.senders.read().await.get(&bot_id).cloned() {
            return sender;
        }
        let mut senders = self.senders.write().await;
        senders
            .entry(bot_id)
            .or_insert_with(|| broadcast::channel(1024).0)
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use super::{EventBus, PendingMediaCapabilities, StreamLimiter};
    use uuid::Uuid;

    #[tokio::test]
    async fn ingestion_lock_serializes_each_bot() {
        let events = EventBus::default();
        let bot_id = Uuid::new_v4();
        let first = events.lock_ingestion(bot_id).await;
        let contender = events.clone();
        let (acquired, mut receiver) = tokio::sync::mpsc::channel(1);
        let task = tokio::spawn(async move {
            let _second = contender.lock_ingestion(bot_id).await;
            acquired.send(()).await.expect("receiver remains open");
        });

        tokio::task::yield_now().await;
        assert!(receiver.try_recv().is_err());
        drop(first);
        receiver.recv().await.expect("second lock acquired");
        task.await.expect("contender task completed");
    }

    #[test]
    fn stream_limiter_enforces_global_and_per_key_caps() {
        let limiter = StreamLimiter::with_limits(2, 1);
        let first = limiter.try_acquire(b"first").expect("first permit");
        assert!(limiter.try_acquire(b"first").is_err());
        let second = limiter.try_acquire(b"second").expect("second permit");
        assert!(limiter.try_acquire(b"third").is_err());

        drop(first);
        let replacement = limiter
            .try_acquire(b"first")
            .expect("released permit is reusable");
        drop((second, replacement));
    }

    #[test]
    fn pending_media_capability_is_scoped_to_the_bot() {
        let capabilities = PendingMediaCapabilities::default();
        let owner = Uuid::new_v4();
        let other = Uuid::new_v4();
        capabilities.authorize(owner, ["owned-file"]);

        assert!(capabilities.contains(owner, "owned-file"));
        assert!(!capabilities.contains(other, "owned-file"));
        assert!(!capabilities.contains(owner, "not-owned"));
    }
}
