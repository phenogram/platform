use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex as StdMutex, Weak},
    time::{Duration, Instant},
};

use sqlx::PgPool;
use tokio::sync::{Mutex, OwnedSemaphorePermit, RwLock, Semaphore, broadcast};
use uuid::Uuid;

use crate::{config::Config, crypto::Crypto};

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub db: PgPool,
    pub crypto: Arc<Crypto>,
    pub telegram: reqwest::Client,
    pub events: EventBus,
    pub auth_limiter: AuthLimiter,
    pub stream_limiter: StreamLimiter,
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
        Ok(Self {
            config: Arc::new(config),
            db,
            crypto: Arc::new(crypto),
            telegram,
            events: EventBus::default(),
            auth_limiter: AuthLimiter::default(),
            stream_limiter: StreamLimiter::default(),
        })
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
    hashing_slots: Arc<Semaphore>,
}

impl Default for AuthLimiter {
    fn default() -> Self {
        Self {
            attempts: Arc::new(Mutex::new(HashMap::new())),
            hashing_slots: Arc::new(Semaphore::new(4)),
        }
    }
}

impl AuthLimiter {
    pub async fn check(&self, source: &str, email: &str) -> crate::error::Result<()> {
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
            (format!("source-email:{source}:{email}"), 8_usize),
        ] {
            let entries = attempts.entry(key).or_default();
            if entries.len() >= limit {
                return Err(crate::error::AppError::RateLimited);
            }
            entries.push_back(now);
        }
        Ok(())
    }

    pub fn hashing_slot(&self) -> crate::error::Result<OwnedSemaphorePermit> {
        self.hashing_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| crate::error::AppError::RateLimited)
    }
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct StoredUpdate {
    pub row_id: i64,
    pub update_id: i64,
    pub event_type: String,
    pub payload: serde_json::Value,
}

#[derive(Clone, Default)]
pub struct EventBus {
    senders: Arc<RwLock<HashMap<Uuid, broadcast::Sender<StoredUpdate>>>>,
}

impl EventBus {
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
    use super::StreamLimiter;

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
}
