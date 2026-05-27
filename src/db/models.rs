use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Collection {
    pub id: String,
    pub name: String,
    pub description: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrawlTarget {
    pub id: String,
    pub collection_id: String,
    pub url_prefix: String,
    pub recrawl_interval_s: i64,
    pub last_crawled_at: Option<DateTime<Utc>>,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutlinkSuggestion {
    pub id: String,
    pub collection_id: String,
    pub source_url: String,
    pub target_url: String,
    pub link_text: String,
    pub first_seen_at: DateTime<Utc>,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodePeer {
    pub id: String,
    pub url: String,
    pub label: String,
    pub enabled: bool,
    pub consecutive_failures: i64,
    pub last_seen_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionPeer {
    pub id: String,
    pub local_collection_id: String,
    pub node_peer_id: String,
    pub remote_collection_name: String,
    pub weight: f64,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveredEngine {
    pub id: String,
    pub url: String,
    pub label: String,
    pub description: String,
    pub first_seen_at: DateTime<Utc>,
    pub last_seen_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RankingConfig {
    pub collection_id: String,
    pub local_source_weight: f64,
    pub peer_source_weight: f64,
    pub freshness_half_life_days: f64,
    pub domain_boosts_json: String,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitState {
    pub kind: String,
    pub key: String,
    pub window_start: DateTime<Utc>,
    pub request_count: i64,
    pub cooloff_until: Option<DateTime<Utc>>,
    pub cooloff_tier: i64,
    pub last_violation_at: Option<DateTime<Utc>>,
}
