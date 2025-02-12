use serde::Serialize;
use sqlx::mysql::{MySqlPoolOptions, MySqlQueryResult, MySqlRow};
use sqlx::{Error, MySqlPool};
use uuid::Uuid;

pub struct DbRepository {
    pool: MySqlPool,
}

//TODO: provide a repeating function to retry requests
impl DbRepository {
    pub fn new(pool: MySqlPool) -> Self {
        Self { pool }
    }
    pub async fn start_transaction(&self) -> sqlx::Result<sqlx::Transaction<'_, sqlx::MySql>> {
        self.pool.begin().await
    }

    pub async fn lock_for_update(
        // &self,
        worker_uuid: &String,
        num_workers: u32,
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
            num_workers,
        )
            .fetch_all(&mut *tx)
            .await
    }

    pub async fn update_worker_uuid(
        // &self,
        worker_uuid: &String,
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

    pub async fn mark_as_processed(&self, record_id: &String) -> Result<MySqlQueryResult, Error> {
        // let record_id = Uuid::parse_str(record_id).unwrap();
        let record_id = record_id.as_bytes().to_vec();
        sqlx::query!(
            "UPDATE processing_transactional_outbox SET is_processed = 1 WHERE id = ?",
            record_id
        )
        .execute(&self.pool)
        .await
    }

    pub async fn worker_records(&self, worker_uuid: &String) -> Result<Vec<MySqlRow>, Error> {
        let worker_id = Uuid::parse_str(worker_uuid).unwrap();
        let sql = format!(
            "SELECT * FROM processing_transactional_outbox WHERE worker_uuid = '{}' AND is_processed = 0 ORDER BY created_at",
            worker_id
        );
        // sqlx::query(
        //     // SeChangeOutbox,
        //     // WorkerOutboxRecord,
        //     // "SELECT id, source_estate_id, type_name, payload \
        //     "SELECT * \
        //     FROM processing_transactional_outbox \
        //     WHERE worker_uuid = ? AND is_processed = 0 \
        //     ORDER BY created_at",
        //     worker_uuid,
        // )
        sqlx::query(&sql).fetch_all(&self.pool).await
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
    pub async fn create_pool(max_connections: u32, db_url: &str) -> Result<MySqlPool, Error> {
        MySqlPoolOptions::new()
            .max_connections(max_connections)
            .connect(db_url)
            .await
    }
}

#[derive(Debug, Serialize)]
pub struct TransactionalOutboxId {
    pub id: Vec<u8>,
}
