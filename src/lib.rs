use crate::RecordProcessorError::{
    LockForUpdateError, StartTransactionError, UnableToMarkRecordAsProcessedError,
    UnableToSetWorkerUuidError, WorkerRecordsRetrievalError,
};
use db_repository::DbRepository;
use formatted_logger::HashMapLogData;
use log::{debug, error, info, warn};
use sqlx::mysql::{MySqlQueryResult, MySqlRow};
use sqlx::{Error, Row};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use uuid::Uuid;

pub mod db_repository;

pub struct TransactionalOutbox {
    pub id: String,
    pub payload: String,
}

#[derive(Debug)]
pub enum RecordProcessorError {
    StartTransactionError,
    LockForUpdateError,
    UnableToSetWorkerUuidError,
    WorkerRecordsRetrievalError,
    UnableToMarkRecordAsProcessedError,
    RecordProcessingError,
}

pub async fn run(processor: Arc<dyn AsyncOutboxProcessor + Send + Sync>, log_data: HashMapLogData) {
    let mut log_data = log_data.clone();
    log_data.insert("relay_pkg_version", env!("CARGO_PKG_VERSION").to_string());
    info!(target: "run", ctxt = log_data; "Starting the transaction outbox processor...");

    info!(target: "run", ctxt = log_data; "Resetting worker_uuid field for unprocessed records...");
    //reset worker_uuid field for unprocessed records
    let _ = processor.db_repository().reset_worker_ids(&log_data).await;
    info!(target: "run", ctxt = log_data; "worker_uuid field for unprocessed records has been reset");

    let num_workers = processor.num_workers();
    let mut handles = vec![];
    for _ in 0..num_workers {
        let cloned_data = log_data.clone();
        let processor = processor.clone();
        handles.push(tokio::spawn(async move {
            let log_data = cloned_data.clone();
            let error = process_records(processor, log_data).await;
            let log_data2 = cloned_data.clone();
            error!(target: "run:thread_handler", ctxt = log_data2; "{}", format!("Error: {:?}", error));
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }
}

pub trait OutboxProcessor: Send + Sync {
    fn num_workers(&self) -> u32;
    fn map_record(&self, record: &MySqlRow) -> TransactionalOutbox {
        TransactionalOutbox {
            id: String::from_utf8(record.get("id")).unwrap(),
            payload: String::new(),
        }
    }
    fn db_repository(&self) -> &DbRepository;
}

#[async_trait::async_trait]
pub trait AsyncOutboxProcessor: OutboxProcessor {
    async fn process_record(
        &self,
        outbox: &TransactionalOutbox,
        record: &MySqlRow,
        log_data: HashMapLogData,
    ) -> Result<(), RecordProcessorError>;
}

async fn process_records(
    record_processor: Arc<dyn AsyncOutboxProcessor + Sync + Send>,
    log_data: HashMapLogData,
) -> RecordProcessorError {
    // let query_string = format!("SELECT id, source_estate_id, type_name, payload FROM processing_transactional_outbox WHERE is_processed = 0 LIMIT {}", NUM_CONSUMERS);
    let worker_uuid = Uuid::new_v4().to_string();

    let mut log_data = log_data.clone();
    // let mut log_data = log_hashmap!("key" => "value");
    log_data.insert("worker_uuid", worker_uuid.clone());

    let record_processor = record_processor.clone();

    let rows_limit = record_processor.db_repository().max_rows_limit();

    debug!(target: "run:thread_handler:process_records", ctxt = log_data; "Starting a worker loop...");
    loop {
        //start a transaction
        debug!(target: "run:thread_handler:process_records", ctxt = log_data; "Starting a transaction...");
        let tx_result = record_processor
            .db_repository()
            .start_transaction(&log_data)
            .await;
        let mut tx = match tx_result {
            Ok(tx) => tx,
            Err(_) => {
                error!(target: "run:thread_handler:process_records", ctxt = log_data; "{}", format!("Failed to start a transaction: {:?}", tx_result));
                // return Err(StartTransactionError);
                return StartTransactionError;
            }
        };

        // // Set lock wait timeout
        // sqlx::query("SET innodb_lock_wait_timeout = 50")
        //     .execute(&mut tx)
        //     .await
        //     .unwrap();

        //lock records for update
        let records_result = record_processor
            .db_repository()
            .lock_for_update(&worker_uuid, &mut tx, &log_data)
            .await;
        let records = match records_result {
            Ok(records) => records,
            Err(_) => {
                error!(target: "run:thread_handler:process_records", ctxt = log_data; "{}", format!("Failed to lock for update: {:?}", records_result));
                // return Err(LockForUpdateError);
                return LockForUpdateError;
            }
        };

        let last_fetched_records_count = records.len();

        info!(target: "run:thread_handler:process_records", ctxt = log_data; "Got {} locked records", records.len());

        //set worker_uuid for the records
        for record in records {
            let record_id = &String::from_utf8(record.id.clone()).unwrap();

            let res: Result<MySqlQueryResult, Error> = record_processor
                .db_repository()
                .update_worker_uuid(&worker_uuid, record.id.clone(), &mut tx, &log_data)
                .await;

            let res = match res {
                Ok(res) => res,
                Err(_) => {
                    error!(target: "run:thread_handler:process_records", ctxt = log_data; "{}", format!("Failed to update worker_uuid for record {} before the commit: {:?}", record_id, res));
                    // return Err(UnableToSetWorkerUuidError);
                    return UnableToSetWorkerUuidError;
                }
            };

            if res.rows_affected() == 0 {
                warn!(target: "run:thread_handler:process_records", ctxt = log_data; "Has not changed worker_uuid for record {}", record_id);
                continue;
            }

            debug!(target: "run:thread_handler:process_records", ctxt = log_data; "A worker_uuid is changed for record {}", record_id);
        }

        //commit the transaction
        debug!(target: "run:thread_handler:process_records", ctxt = log_data; "Commiting the transaction...");
        tx.commit().await.unwrap();
        debug!(target: "run:thread_handler:process_records", ctxt = log_data; "Transaction commited");

        //retrieve worker records
        debug!(target: "run:thread_handler:process_records", ctxt = log_data; "Retrieving marked records...");
        let records_result = record_processor
            .db_repository()
            .worker_records(worker_uuid.as_str(), &log_data)
            .await;
        let records = match records_result {
            Ok(records) => records,
            Err(_) => {
                error!(target: "run:thread_handler:process_records", ctxt = log_data; "{}", format!("Failed to retrieve records: {:?}", records_result));
                // return Err(WorkerRecordsRetrievalError);
                return WorkerRecordsRetrievalError;
            }
        };
        debug!(target: "run:thread_handler:process_records", ctxt = log_data; "Got {} records to process", records.len());

        //performing message records
        for record in records {
            let outbox_record = record_processor.map_record(&record);
            let record_id = &outbox_record.id;

            debug!(target: "run:thread_handler:process_records", ctxt = log_data; "Processing record {}...", record_id);
            let res = &record_processor
                .process_record(&outbox_record, &record, log_data.clone())
                .await;
            if res.is_err() {
                error!(target: "run:thread_handler:process_records", ctxt = log_data; "Failed to process record {}", record_id);
                continue;
            }

            debug!(target: "run:thread_handler:process_records", ctxt = log_data; "The record {} has been processed ", record_id);
            let res = record_processor
                .db_repository()
                .mark_as_processed(record_id, &log_data)
                .await;
            if res.is_err() {
                error!(target: "run:thread_handler:process_records", ctxt = log_data; "Failed to mark record {} as processed: {:?}", record_id, res);
                // return Err(UnableToMarkRecordAsProcessedError);
                return UnableToMarkRecordAsProcessedError;
            }
        }

        let sleep_seconds = match last_fetched_records_count {
            x if x == (rows_limit as usize) => 1,
            _ => 5,
        };

        sleep(Duration::from_secs(sleep_seconds)).await;
    }
}
