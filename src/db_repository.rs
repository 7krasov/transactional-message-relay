use serde::Serialize;
use sqlx::mysql::{MySqlPoolOptions, MySqlQueryResult, MySqlRow};
use sqlx::{Error, MySqlPool, Row};
use std::ops::DerefMut;
use uuid::Uuid;

pub struct DbRepository {
    pool: MySqlPool,
    table_name: String,
    max_rows_limit: u16,
}

//TODO: provide a repeating function to retry requests
impl DbRepository {
    pub fn new(pool: MySqlPool, table_name: String, max_rows_limit: u16) -> Self {
        Self {
            pool,
            table_name,
            max_rows_limit,
        }
    }
    pub async fn start_transaction(&self) -> sqlx::Result<sqlx::Transaction<'_, sqlx::MySql>> {
        self.pool.begin().await
    }

    pub async fn lock_for_update(
        &self,
        worker_uuid: &String,
        tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    ) -> Result<Vec<TransactionalOutboxId>, Error> {
        //it is better to use "FOR UPDATE SKIP LOCKED" but TiDB does not support it https://github.com/pingcap/tidb/issues/18207
        let query = format!(
            "SELECT id FROM {} WHERE worker_uuid IS NULL OR (worker_uuid = '{}' AND is_processed = 0) ORDER BY created_at LIMIT {} FOR UPDATE",
            self.table_name,
            worker_uuid,
            self.max_rows_limit
        );

        let rows = sqlx::query(&query).fetch_all(tx.deref_mut()).await?;

        rows.iter()
            .map(|row| Ok(TransactionalOutboxId { id: row.get("id") }))
            .collect()
        // TransactionalOutboxId {
        //     id: String::from_utf8(record.get("id")).unwrap(),
        // }
    }

    pub async fn update_worker_uuid(
        &self,
        worker_uuid: &String,
        record_id: Vec<u8>,
        tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    ) -> Result<MySqlQueryResult, Error> {
        let query = format!(
            "UPDATE {} SET worker_uuid = '{}' WHERE id = '{}'",
            self.table_name,
            worker_uuid,
            &String::from_utf8(record_id).unwrap()
        );

        sqlx::query(&query).execute(tx.deref_mut()).await
    }

    pub async fn mark_as_processed(&self, record_id: &String) -> Result<MySqlQueryResult, Error> {
        // let record_id = Uuid::parse_str(record_id).unwrap();
        // let record_id = record_id.as_bytes().to_vec();
        let query = format!(
            "UPDATE {} SET is_processed = 1 WHERE id = '{}'",
            self.table_name, record_id
        );
        sqlx::query(&query).execute(&self.pool).await
    }

    pub async fn worker_records(&self, worker_uuid: &str) -> Result<Vec<MySqlRow>, Error> {
        let worker_id = Uuid::parse_str(worker_uuid).unwrap();
        let sql = format!(
            "SELECT * FROM {} WHERE worker_uuid = '{}' AND is_processed = 0 ORDER BY created_at",
            self.table_name, worker_id
        );
        sqlx::query(&sql).fetch_all(&self.pool).await
    }

    pub async fn reset_worker_ids(&self) {
        let query = format!(
            "UPDATE {} SET worker_uuid = NULL WHERE is_processed = 0",
            self.table_name
        );

        sqlx::query(&query)
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
