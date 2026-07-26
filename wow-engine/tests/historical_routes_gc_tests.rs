use chrono::{Duration as ChronoDuration, Utc};
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;
use uuid::Uuid;
use wow_engine::gc::{run_once, GcConfig};

async fn setup_db() -> Result<sqlx::postgres::PgPool, sqlx::Error> {
    let database_url = std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost/wow_engine_test".to_string());

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&database_url)
        .await?;

    sqlx::migrate!("./migrations").run(&pool).await.ok();

    Ok(pool)
}

async fn cleanup_db(pool: &sqlx::postgres::PgPool) -> Result<(), sqlx::Error> {
    sqlx::query("TRUNCATE historical_routes CASCADE")
        .execute(pool)
        .await
        .ok();
    Ok(())
}

/// Inserts one `historical_routes` row with an explicit `archived_at`,
/// bypassing application code so tests can control staleness directly.
async fn insert_row(
    pool: &sqlx::postgres::PgPool,
    archived_at: chrono::DateTime<Utc>,
) -> Result<Uuid, sqlx::Error> {
    let id = Uuid::now_v7();
    sqlx::query(
        r#"
        INSERT INTO historical_routes
        (id, original_route_execution_id, user_id, source_chain, dest_chain,
         source_asset, dest_asset, amount_in, amount_out, provider, path,
         estimated_fee_usd, final_status, archived_at, original_created_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
        "#,
    )
    .bind(id)
    .bind(Uuid::now_v7())
    .bind(Uuid::now_v7())
    .bind("Ethereum")
    .bind("Stellar")
    .bind("USDC")
    .bind("USDC")
    .bind(100_000_000_i64)
    .bind(99_500_000_i64)
    .bind("CCTP")
    .bind("Ethereum -> Stellar via CCTP")
    .bind(5.0_f64)
    .bind("executed")
    .bind(archived_at)
    .bind(archived_at)
    .execute(pool)
    .await?;

    Ok(id)
}

fn test_config() -> GcConfig {
    GcConfig {
        interval: Duration::from_secs(86_400),
        retention: ChronoDuration::days(7),
        batch_size: 2,
        batch_delay: Duration::from_millis(1),
    }
}

#[tokio::test]
#[ignore]
async fn deletes_only_rows_older_than_retention() {
    let pool = match setup_db().await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("Skipping test: {}", e);
            return;
        }
    };
    let _ = cleanup_db(&pool).await;

    let stale = Utc::now() - ChronoDuration::days(10);
    let fresh = Utc::now() - ChronoDuration::days(1);

    let stale_id = insert_row(&pool, stale).await.expect("insert stale row");
    let fresh_id = insert_row(&pool, fresh).await.expect("insert fresh row");

    let stats = run_once(&pool, &test_config())
        .await
        .expect("gc run should succeed");

    assert_eq!(
        stats.rows_deleted, 1,
        "only the stale row should be removed"
    );

    let stale_count: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM historical_routes WHERE id = $1")
            .bind(stale_id)
            .fetch_one(&pool)
            .await
            .expect("count stale row");
    assert_eq!(stale_count.0, 0, "stale row must be deleted");

    let fresh_count: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM historical_routes WHERE id = $1")
            .bind(fresh_id)
            .fetch_one(&pool)
            .await
            .expect("count fresh row");
    assert_eq!(fresh_count.0, 1, "fresh row must survive");

    let _ = cleanup_db(&pool).await;
}

#[tokio::test]
#[ignore]
async fn deletes_stale_rows_across_multiple_batches() {
    let pool = match setup_db().await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("Skipping test: {}", e);
            return;
        }
    };
    let _ = cleanup_db(&pool).await;

    let stale = Utc::now() - ChronoDuration::days(30);
    for _ in 0..5 {
        insert_row(&pool, stale).await.expect("insert stale row");
    }

    // batch_size = 2, so 5 stale rows requires 3 batches to fully clear.
    let stats = run_once(&pool, &test_config())
        .await
        .expect("gc run should succeed");

    assert_eq!(stats.rows_deleted, 5);
    assert_eq!(stats.batches, 3);

    let remaining: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM historical_routes")
        .fetch_one(&pool)
        .await
        .expect("count remaining rows");
    assert_eq!(remaining.0, 0);

    let _ = cleanup_db(&pool).await;
}

#[tokio::test]
#[ignore]
async fn run_once_surfaces_error_on_closed_pool() {
    let pool = match setup_db().await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("Skipping test: {}", e);
            return;
        }
    };

    pool.close().await;

    let result = run_once(&pool, &test_config()).await;
    assert!(
        result.is_err(),
        "a closed pool must surface an error instead of panicking"
    );
}
