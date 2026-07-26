//! Background garbage collection for the `historical_routes` archive.
//!
//! Every completed route execution is archived into `historical_routes`, so
//! the table grows without bound as long as the engine serves traffic.
//! Left unchecked this bloats table/index size and degrades read latency
//! until the database falls over. This module runs a Tokio task that wakes
//! up on a fixed interval and deletes rows older than a configured retention
//! window, in small batches so the table never sits behind a single
//! long-held exclusive lock.

use crate::db::Database;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use sqlx::postgres::PgPool;
use std::time::Duration;

/// Tunables for the historical routes GC worker.
#[derive(Debug, Clone, Copy)]
pub struct GcConfig {
    /// How long to sleep between GC runs.
    pub interval: Duration,
    /// Rows archived longer ago than this are eligible for deletion.
    pub retention: ChronoDuration,
    /// Maximum rows removed by a single `DELETE` statement.
    pub batch_size: i64,
    /// Pause between batches, giving other queries a chance at the table.
    pub batch_delay: Duration,
}

impl GcConfig {
    pub fn from_app_config(config: &crate::config::AppConfig) -> Self {
        Self {
            interval: Duration::from_secs(config.gc_interval_secs),
            retention: ChronoDuration::days(config.gc_retention_days),
            batch_size: config.gc_batch_size,
            batch_delay: Duration::from_millis(config.gc_batch_delay_ms),
        }
    }
}

/// Outcome of a single GC pass, reported to the observability stack.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct GcRunStats {
    pub rows_deleted: u64,
    pub batches: u32,
}

/// Deletes at most one batch of stale rows and returns how many were removed.
///
/// Rows are selected by `archived_at` — the moment they entered this
/// archive table — via a bounded subquery, so the resulting `DELETE` only
/// ever locks the `batch_size` rows it actually removes instead of scanning
/// and locking the whole table.
async fn delete_batch(
    pool: &PgPool,
    cutoff: DateTime<Utc>,
    batch_size: i64,
) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(
        r#"
        DELETE FROM historical_routes
        WHERE id IN (
            SELECT id FROM historical_routes
            WHERE archived_at < $1
            ORDER BY archived_at
            LIMIT $2
        )
        "#,
    )
    .bind(cutoff)
    .bind(batch_size)
    .execute(pool)
    .await?;

    Ok(result.rows_affected())
}

/// Runs one full GC pass: repeatedly deletes batches of stale rows, pausing
/// between each, until a batch comes back smaller than `batch_size` (i.e.
/// nothing stale is left).
pub async fn run_once(pool: &PgPool, config: &GcConfig) -> Result<GcRunStats, sqlx::Error> {
    let cutoff = Utc::now() - config.retention;
    let mut stats = GcRunStats::default();

    loop {
        let deleted = delete_batch(pool, cutoff, config.batch_size).await?;
        stats.rows_deleted += deleted;
        stats.batches += 1;

        if deleted < config.batch_size as u64 {
            break;
        }

        tokio::time::sleep(config.batch_delay).await;
    }

    Ok(stats)
}

/// Spawns the GC worker as a detached Tokio task.
///
/// The task loops forever on `config.interval`. A failed run (e.g. the
/// database connection drops mid-batch) is logged and swallowed rather than
/// propagated: the worker simply tries again on the next tick, so a
/// transient outage never takes down the process or permanently disables
/// pruning.
pub fn spawn(db: Database, config: GcConfig) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(config.interval);
        // The first tick fires immediately; skip it so GC doesn't run the
        // instant the process boots, and instead waits a full interval.
        ticker.tick().await;

        loop {
            ticker.tick().await;
            let started = tokio::time::Instant::now();

            match run_once(db.pool(), &config).await {
                Ok(stats) => {
                    tracing::info!(
                        rows_deleted = stats.rows_deleted,
                        batches = stats.batches,
                        duration_ms = started.elapsed().as_millis() as u64,
                        "historical_routes GC pass completed"
                    );
                }
                Err(err) => {
                    tracing::error!(
                        error = %err,
                        duration_ms = started.elapsed().as_millis() as u64,
                        "historical_routes GC pass failed; will retry next interval"
                    );
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_app_config_maps_all_fields() {
        let app_config = crate::config::AppConfig {
            gc_interval_secs: 3600,
            gc_retention_days: 3,
            gc_batch_size: 100,
            gc_batch_delay_ms: 25,
            ..Default::default()
        };

        let gc_config = GcConfig::from_app_config(&app_config);

        assert_eq!(gc_config.interval, Duration::from_secs(3600));
        assert_eq!(gc_config.retention, ChronoDuration::days(3));
        assert_eq!(gc_config.batch_size, 100);
        assert_eq!(gc_config.batch_delay, Duration::from_millis(25));
    }

    #[test]
    fn run_stats_default_is_zeroed() {
        let stats = GcRunStats::default();
        assert_eq!(stats.rows_deleted, 0);
        assert_eq!(stats.batches, 0);
    }
}
