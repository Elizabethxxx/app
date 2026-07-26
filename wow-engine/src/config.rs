use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
pub struct AppConfig {
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default)]
    pub database_url: Option<String>,
    /// Hex-encoded 32-byte Ed25519 public key of trusted internal callers.
    ///
    /// When set, all non-public endpoints require a valid `X-Signature`.
    /// When unset, internal request-signature verification is disabled — safe
    /// only for local development.
    #[serde(default)]
    pub signing_public_key: Option<String>,
    /// Upper bound, in seconds, on how long any single HTTP request may run
    /// before the server aborts it and returns `408 Request Timeout`.
    ///
    /// This is the outermost backstop against a hung downstream dependency
    /// pinning a request (and its resources) open indefinitely. Individual
    /// dependencies enforce their own, tighter timeouts via the resilience
    /// layer; this guarantees a request can never outlive it.
    #[serde(default = "default_request_timeout_secs")]
    pub request_timeout_secs: u64,
    /// Connection string for the Redis instance used to broadcast cache
    /// invalidations across nodes (e.g. `redis://localhost:6379`).
    ///
    /// When unset, the engine runs in single-node mode: it never publishes or
    /// subscribes to invalidation messages and relies purely on local cache
    /// TTLs. This is also the fallback behavior if Redis is configured but
    /// becomes unreachable at runtime — a missing/broken Redis must never
    /// crash the engine.
    #[serde(default)]
    pub redis_url: Option<String>,
    /// JSON-RPC endpoint used to read Circle CCTP attester keys on-chain.
    #[serde(default = "default_eth_rpc_url")]
    pub eth_rpc_url: String,
    /// Address of Circle's `MessageTransmitter` contract on the source chain.
    #[serde(default = "default_cctp_message_transmitter")]
    pub cctp_message_transmitter: String,
    /// Path of the append-only log recording consumed CCTP nonces, so
    /// replay protection survives restarts and redeploys.
    #[serde(default = "default_cctp_nonce_store_path")]
    pub cctp_nonce_store_path: String,
    /// How often, in seconds, the historical routes garbage collector wakes
    /// up to prune stale rows. Defaults to 24 hours.
    #[serde(default = "default_gc_interval_secs")]
    pub gc_interval_secs: u64,
    /// Age, in days, after which a `historical_routes` row is considered
    /// stale and eligible for deletion.
    #[serde(default = "default_gc_retention_days")]
    pub gc_retention_days: i64,
    /// Maximum number of rows deleted per batch by the GC worker. Keeping
    /// this small bounds how long any single `DELETE` holds row locks, so
    /// the table stays available for concurrent reads.
    #[serde(default = "default_gc_batch_size")]
    pub gc_batch_size: i64,
    /// Delay, in milliseconds, between successive delete batches.
    #[serde(default = "default_gc_batch_delay_ms")]
    pub gc_batch_delay_ms: u64,
}

fn default_port() -> u16 {
    8080
}

fn default_request_timeout_secs() -> u64 {
    30
}

fn default_eth_rpc_url() -> String {
    "https://ethereum-rpc.publicnode.com".to_string()
}

fn default_cctp_message_transmitter() -> String {
    // Circle MessageTransmitter on Ethereum mainnet.
    "0x0a992d191deec32afe36203ad87d7d289a738f81".to_string()
}

fn default_cctp_nonce_store_path() -> String {
    "data/cctp_consumed_nonces.log".to_string()
}

fn default_gc_interval_secs() -> u64 {
    24 * 60 * 60
}

fn default_gc_retention_days() -> i64 {
    7
}

fn default_gc_batch_size() -> i64 {
    5000
}

fn default_gc_batch_delay_ms() -> u64 {
    50
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            port: default_port(),
            database_url: None,
            signing_public_key: None,
            request_timeout_secs: default_request_timeout_secs(),
            redis_url: None,
            eth_rpc_url: default_eth_rpc_url(),
            cctp_message_transmitter: default_cctp_message_transmitter(),
            cctp_nonce_store_path: default_cctp_nonce_store_path(),
            gc_interval_secs: default_gc_interval_secs(),
            gc_retention_days: default_gc_retention_days(),
            gc_batch_size: default_gc_batch_size(),
            gc_batch_delay_ms: default_gc_batch_delay_ms(),
        }
    }
}

impl AppConfig {
    pub fn load() -> Result<Self, envy::Error> {
        envy::from_env::<AppConfig>()
    }

    pub fn get_database_url(&self) -> anyhow::Result<String> {
        self.database_url.clone().ok_or_else(|| {
            anyhow::anyhow!(
                "DATABASE_URL environment variable not set. \
                 Example: postgres://postgres:postgres@localhost/wow_engine"
            )
        })
    }
}
