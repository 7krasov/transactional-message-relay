use serde::Serialize;
use sqlx::mysql::{MySqlPoolOptions, MySqlQueryResult};
use sqlx::{Error, MySqlPool};
use std::env;

pub struct DbRepository {
    pool: MySqlPool,
}

const ENV_DATABASE_URL: &str = "DATABASE_URL";

impl DbRepository {
    pub async fn new(pool: MySqlPool) -> DbRepository {
        DbRepository { pool }
    }
    pub async fn start_transaction(&self) -> sqlx::Result<sqlx::Transaction<'_, sqlx::MySql>> {
        self.pool.begin().await
    }

    pub async fn lock_for_update(
        worker_uuid: String,
        tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    ) -> Result<Vec<TransactionalOutboxId>, Error> {
        sqlx::query_as!(
            TransactionalOutboxId,
            // "SELECT id, source_estate_id, type_name, payload \
            "SELECT id \
            FROM processing_transactional_outbox \
            WHERE worker_uuid IS NULL OR (worker_uuid = ? AND is_processed = 0) ORDER BY created_at ASC LIMIT ? \
            FOR UPDATE",
            //it is better to use "FOR UPDATE SKIP LOCKED" but TiDB does not support it https://github.com/pingcap/tidb/issues/18207
            worker_uuid,
            crate::NUM_ROWS_LIMIT,
        )
            .fetch_all(&mut *tx)
            .await
    }

    pub async fn update_worker_uuid(
        worker_uuid: String,
        record_id: Vec<u8>,
        tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    ) -> Result<MySqlQueryResult, Error> {
        sqlx::query!(
            "UPDATE processing_transactional_outbox SET worker_uuid = ? WHERE id = ?",
            worker_uuid,
            record_id
        )
        .execute(&mut *tx)
        .await
    }

    pub async fn mark_as_processed(&self, record_id: Vec<u8>) -> Result<MySqlQueryResult, Error> {
        sqlx::query!(
            "UPDATE processing_transactional_outbox SET is_processed = 1 WHERE id = ?",
            record_id
        )
        .execute(&self.pool)
        .await
    }

    pub async fn worker_records(
        &self,
        worker_uuid: String,
    ) -> Result<Vec<TransactionalOutbox>, Error> {
        sqlx::query_as!(
            TransactionalOutbox,
            "SELECT id, source_estate_id, type_name, payload \
            FROM processing_transactional_outbox \
            WHERE worker_uuid = ? AND is_processed = 0 \
            ORDER BY created_at",
            worker_uuid
        )
        .fetch_all(&self.pool)
        .await
    }

    pub async fn reset_worker_ids(&self) {
        sqlx::query!(
            "UPDATE processing_transactional_outbox SET worker_uuid = NULL WHERE is_processed = 0"
        )
        .execute(&self.pool)
        .await
        .expect("failed to reset worker_uuids");
    }
}

pub struct PoolFactory {}

impl PoolFactory {
    pub async fn create_pool(max_connections: u32) -> Result<MySqlPool, Error> {
        let db_url = env::var(ENV_DATABASE_URL);
        if db_url.is_err() {
            panic!("{}", format!("{} is not set", ENV_DATABASE_URL));
        }
        let pool = MySqlPoolOptions::new()
            .max_connections(max_connections)
            .connect(&db_url.unwrap())
            .await?;
        Ok(pool)
    }
}

#[derive(Debug, Serialize)]
pub struct TransactionalOutbox {
    pub id: Vec<u8>,
    pub source_estate_id: u64,
    pub type_name: String,
    payload: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct TransactionalOutboxId {
    pub id: Vec<u8>,
}
