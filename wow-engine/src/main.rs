use std::net::SocketAddr;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    tracing::info!("Shutdown signal received, starting graceful shutdown");
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing subscriber
    tracing_subscriber::fmt::init();

    // Load environment variables from .env file
    dotenvy::dotenv().ok();

    // Load strongly-typed configuration
    let config = wow_engine::config::AppConfig::load()?;

    // 1. Connect to the database (required for route execution/quota tracking)
    let db = match config.get_database_url() {
        Ok(url) => match wow_engine::db::Database::new(&url).await {
            Ok(db) => {
                // Apply any pending schema migrations before serving traffic.
                // This ensures the database schema always matches the binary's expectations,
                // eliminating configuration drift across environments.
                tracing::info!("Running pending database migrations...");
                if let Err(err) = db.run_migrations().await {
                    tracing::error!("Fatal: failed to apply database migrations: {err}");
                    return Err(err.into());
                }
                tracing::info!("Database migrations applied successfully.");

                // Background task: prunes stale historical_routes rows on a
                // fixed interval so the archive table doesn't grow without
                // bound. Runs independently of request traffic and never
                // takes down the process on a transient DB error.
                let gc_config = wow_engine::gc::GcConfig::from_app_config(&config);
                wow_engine::gc::spawn(db.clone(), gc_config);
                tracing::info!(
                    "historical_routes GC worker started (interval={}s, retention={}d, batch_size={})",
                    gc_config.interval.as_secs(),
                    config.gc_retention_days,
                    gc_config.batch_size
                );

                Some(db)
            }
            Err(err) => {
                tracing::warn!("Failed to connect to database: {err}. /api/v1/execute-route will be unavailable.");
                None
            }
        },
        Err(err) => {
            tracing::warn!("{err}. /api/v1/execute-route will be unavailable.");
            None
        }
    };

    // 2. Build the Ed25519 signature verifier for internal service-to-service
    //    calls. When no key is configured we run with verification DISABLED and
    //    warn loudly — acceptable for local dev, never for production.
    let verifier = match config.signing_public_key.as_deref() {
        Some(key) => {
            let verifier = wow_engine::api::auth::SignatureVerifier::from_hex_public_key(key)?;
            tracing::info!("Ed25519 request-signature verification ENABLED for internal endpoints");
            Some(verifier)
        }
        None => {
            tracing::warn!(
                "SIGNING_PUBLIC_KEY not set: internal request-signature verification is DISABLED. \
                 Protected endpoints are unauthenticated. Do NOT run this way in production."
            );
            None
        }
    };

    // 3. Build the shared, cluster-aware cache. A single long-lived
    //    `GasOracle` is shared across every request on this node (instead of
    //    each request building its own throwaway one), and — if REDIS_URL is
    //    set — a broadcaster that publishes invalidation events to every
    //    other node. Redis is an optimization, never a hard dependency: a
    //    missing or unreachable Redis just means this node runs on local
    //    TTLs alone, exactly as if REDIS_URL had never been set.
    let gas_oracle = std::sync::Arc::new(wow_engine::bridge::gas_oracle::GasOracle::new());
    let redis_broadcaster = match &config.redis_url {
        Some(url) => match redis::Client::open(url.as_str()) {
            Ok(client) => match client.get_connection_manager().await {
                Ok(manager) => {
                    tracing::info!("Connected to Redis for cluster-wide cache invalidation");
                    Some(std::sync::Arc::new(
                        wow_engine::cache_sync::CacheInvalidationBroadcaster::new(
                            Some(manager),
                            wow_engine::cache_sync::CACHE_INVALIDATION_CHANNEL,
                        ),
                    ))
                }
                Err(err) => {
                    tracing::warn!(
                        "Failed to connect to Redis ({err}); running with local TTL-only caching"
                    );
                    None
                }
            },
            Err(err) => {
                tracing::warn!("Invalid REDIS_URL ({err}); running with local TTL-only caching");
                None
            }
        },
        None => {
            tracing::info!(
                "REDIS_URL not set; running with local TTL-only caching (single-node behavior)"
            );
            None
        }
    };
    let cluster_cache = wow_engine::cache_sync::ClusterCache {
        gas_oracle: gas_oracle.clone(),
        broadcaster: redis_broadcaster,
    };

    // Background task: keeps this node's cache in sync with cluster-wide
    // invalidation events. Reconnects with backoff on its own if Redis is
    // unreachable or drops mid-stream; never crashes the process.
    tokio::spawn(wow_engine::cache_sync::run_redis_subscriber(
        config.redis_url.clone(),
        gas_oracle,
    ));

    // 4. Initialize API router with CORS enabled for seamless frontend calls.
    //    A per-request timeout (configurable via REQUEST_TIMEOUT_SECS) guards
    //    against any single request hanging on a stalled downstream dependency.
    let request_timeout = std::time::Duration::from_secs(config.request_timeout_secs);
    let app =
        wow_engine::api::create_router_with_cache(db, verifier, request_timeout, cluster_cache)
            .layer(CorsLayer::permissive())
            .layer(TraceLayer::new_for_http());

    // 5. Bind TCP listener on configured port
    let port = config.port;
    // Bind all container interfaces so the published Docker port can reach the
    // service. The container still runs as the unprivileged `nonroot` user.
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr).await?;

    tracing::info!("Wow Engine is booting up and routing pipeline conversions...");
    tracing::info!("   Listening on: http://{}", addr);
    tracing::info!("   Endpoints available:");
    tracing::info!("     - GET  /api/v1/health              (Health Check)");
    tracing::info!("     - POST /api/v1/quote               (Quoting Pathfinder)");
    tracing::info!("     - POST /api/v1/execute-route       (Atomic Route Execution)");
    tracing::info!("     - POST /api/v1/anchor/deposit      (SEP-24 Deposit Anchor / On-ramp)");
    tracing::info!("     - POST /api/v1/anchor/withdraw     (SEP-24 Withdraw Anchor / Off-ramp)");
    tracing::info!("     - POST /api/v1/anchor/quote        (SEP-38 Anchor Quotes)");
    tracing::info!("     - POST /api/v1/admin/invalidate-cache (Cluster Cache Invalidation)");

    // 6. Serve incoming TCP requests through Axum pipeline
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}
