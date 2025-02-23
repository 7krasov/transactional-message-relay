use formatted_logger::HashMapLogData;
use log::{error, LevelFilter};
use serde::Serialize;
use sqlx::mysql::{MySqlPoolOptions, MySqlQueryResult, MySqlRow};
use sqlx::{Error, MySqlPool, Row};
use std::ops::DerefMut;
use std::time::Duration;
use tokio::time::sleep;
use uuid::Uuid;

const MAX_RETRIES: u16 = 3;
const RETRY_DELAY_MS: u64 = 1000;

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
    pub async fn start_transaction(
        &self,
        log_data: &HashMapLogData,
    ) -> sqlx::Result<sqlx::Transaction<'_, sqlx::MySql>> {
        execute_transactionless_with_retry(
            || self.pool.begin(),
            MAX_RETRIES,
            Duration::from_millis(RETRY_DELAY_MS),
            log_data,
        )
        .await
    }

    pub async fn lock_for_update(
        &self,
        worker_uuid: &String,
        tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
        log_data: &HashMapLogData,
    ) -> Result<Vec<TransactionalOutboxId>, Error> {
        //it is better to use "FOR UPDATE SKIP LOCKED" but TiDB does not support it https://github.com/pingcap/tidb/issues/18207
        let query = format!(
            "SELECT id FROM {} WHERE worker_uuid IS NULL OR (worker_uuid = '{}' AND is_processed = 0) ORDER BY created_at LIMIT {} FOR UPDATE",
            self.table_name,
            worker_uuid,
            self.max_rows_limit
        );

        let mut attempts = 0;
        loop {
            let result = sqlx::query(&query).fetch_all(tx.deref_mut()).await;

            match result {
                Ok(rows) => {
                    return rows
                        .iter()
                        .map(|row| Ok(TransactionalOutboxId { id: row.get("id") }))
                        .collect();
                    // TransactionalOutboxId {
                    //     id: String::from_utf8(record.get("id")).unwrap(),
                    // }
                }
                Err(e) if attempts < MAX_RETRIES => {
                    error!(target: "lock_for_update", ctxt = log_data; "{:?}", e);
                    attempts += 1;
                    sleep(Duration::from_millis(RETRY_DELAY_MS)).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    pub async fn update_worker_uuid(
        &self,
        worker_uuid: &String,
        record_id: Vec<u8>,
        tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
        log_data: &HashMapLogData,
    ) -> Result<MySqlQueryResult, Error> {
        let query = format!(
            "UPDATE {} SET worker_uuid = '{}' WHERE id = '{}'",
            self.table_name,
            worker_uuid,
            &String::from_utf8(record_id).unwrap()
        );

        let mut attempts = 0;
        loop {
            let result = sqlx::query(&query).execute(tx.deref_mut()).await;

            match result {
                Ok(rows) => return Ok(rows),
                Err(e) if attempts < MAX_RETRIES => {
                    error!(target: "update_worker_uuid", ctxt = log_data; "{:?}", e);
                    attempts += 1;
                    sleep(Duration::from_millis(RETRY_DELAY_MS)).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    pub async fn mark_as_processed(
        &self,
        record_id: &String,
        log_data: &HashMapLogData,
    ) -> Result<MySqlQueryResult, Error> {
        // let record_id = Uuid::parse_str(record_id).unwrap();
        // let record_id = record_id.as_bytes().to_vec();
        let query = format!(
            "UPDATE {} SET is_processed = 1 WHERE id = '{}'",
            self.table_name, record_id
        );
        execute_transactionless_with_retry(
            || sqlx::query(&query).execute(&self.pool),
            MAX_RETRIES,
            Duration::from_millis(RETRY_DELAY_MS),
            log_data,
        )
        .await
    }

    pub async fn worker_records(
        &self,
        worker_uuid: &str,
        log_data: &HashMapLogData,
    ) -> Result<Vec<MySqlRow>, Error> {
        let worker_id = Uuid::parse_str(worker_uuid).unwrap();
        let sql = format!(
            "SELECT * FROM {} WHERE worker_uuid = '{}' AND is_processed = 0 ORDER BY created_at",
            self.table_name, worker_id
        );

        execute_transactionless_with_retry(
            || sqlx::query(&sql).fetch_all(&self.pool),
            MAX_RETRIES,
            Duration::from_millis(RETRY_DELAY_MS),
            log_data,
        )
        .await
    }

    pub async fn reset_worker_ids(&self, log_data: &HashMapLogData) -> Result<(), Error> {
        let query = format!(
            "UPDATE {} SET worker_uuid = NULL WHERE is_processed = 0",
            self.table_name
        );

        execute_transactionless_with_retry(
            // || sqlx::query(&query).execute(&self.pool),
            || async { sqlx::query(&query).execute(&self.pool).await.map(|_| ()) },
            MAX_RETRIES,
            Duration::from_millis(RETRY_DELAY_MS),
            log_data,
        )
        .await
    }
}

pub struct PoolFactory {}

impl PoolFactory {
    pub async fn create_pool(max_connections: u32, db_url: &str) -> Result<MySqlPool, Error> {
        MySqlPoolOptions::new()
            .max_connections(max_connections)
            .acquire_timeout(Duration::from_secs(10))
            .acquire_slow_threshold(Duration::from_secs(5))
            .acquire_slow_level(LevelFilter::Warn)
            .connect(db_url)
            .await
    }
}

#[derive(Debug, Serialize)]
pub struct TransactionalOutboxId {
    pub id: Vec<u8>,
}

async fn execute_transactionless_with_retry<F, Fut, T>(
    mut operation: F,
    max_retries: u16,
    delay: Duration,
    log_data: &HashMapLogData,
) -> Result<T, Error>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, Error>>,
{
    let mut attempts = 0;
    loop {
        match operation().await {
            Ok(result) => return Ok(result),
            Err(e) if attempts < max_retries => {
                error!(target: "execute_transactionless_with_retry", ctxt = log_data; "{:?}", e);
                attempts += 1;
                sleep(delay).await;
            }
            Err(e) => return Err(e),
        }
    }
}
