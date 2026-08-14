use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::FromRow;
use uuid::Uuid;

#[derive(Debug, Clone, FromRow)]
pub struct BotRecord {
    pub id: Uuid,
    pub user_id: Uuid,
    pub telegram_bot_id: i64,
    pub username: String,
    pub display_name: String,
    pub token_ciphertext: Vec<u8>,
    pub token_nonce: Vec<u8>,
    pub token_fingerprint: String,
    pub public_id: String,
    pub ingress_secret_ciphertext: Vec<u8>,
    pub ingress_secret_nonce: Vec<u8>,
    pub status: String,
    pub routing_mode: String,
    pub update_mode: String,
    pub last_update_at: Option<DateTime<Utc>>,
    pub last_api_call_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, FromRow)]
pub struct BotSummary {
    pub id: Uuid,
    pub telegram_bot_id: i64,
    pub username: String,
    pub display_name: String,
    pub public_id: String,
    pub status: String,
    pub routing_mode: String,
    pub update_mode: String,
    pub last_update_at: Option<DateTime<Utc>>,
    pub last_api_call_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub bot_kind: String,
    pub is_managed: bool,
    pub manager_bot_id: Option<Uuid>,
    pub manager_telegram_bot_id: Option<i64>,
    pub manager_username: Option<String>,
    pub manager_display_name: Option<String>,
    pub managed_owner_telegram_user_id: Option<i64>,
    pub plan_covered: bool,
    pub effective_retention_days: i32,
    pub retention_warning: Option<String>,
}

#[derive(Debug, Serialize, FromRow)]
pub struct UpdateSummary {
    pub id: i64,
    pub update_id: i64,
    pub event_type: String,
    pub chat_id: Option<i64>,
    pub telegram_user_id: Option<i64>,
    pub payload: serde_json::Value,
    pub received_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, FromRow)]
pub struct ActivitySummary {
    pub id: i64,
    pub method: String,
    pub source: String,
    pub http_status: Option<i32>,
    pub telegram_ok: Option<bool>,
    pub latency_ms: Option<i32>,
    pub error_summary: Option<String>,
    pub trace_id: Uuid,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, FromRow)]
pub struct ConversationSummary {
    pub chat_id: i64,
    pub chat_type: Option<String>,
    pub title: Option<String>,
    pub username: Option<String>,
    pub display_name: Option<String>,
    pub last_message_preview: Option<String>,
    pub last_update_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, FromRow)]
pub struct PlanMembership {
    pub plan_id: String,
    pub plan_name: String,
    pub status: String,
    pub bot_limit: i32,
    pub retention_days: i32,
    pub local_bot_api: bool,
    pub monthly_price_cents: Option<i32>,
    pub current_period_ends_at: Option<DateTime<Utc>>,
    pub entitlements_active: bool,
}
