mod db_repository;
mod logger;

use db_repository::DbRepository;
use db_repository::PoolFactory;
use log::{debug, error, info, warn};
use logger::init_logger;
use reqwest::Client;
use sqlx::mysql::MySqlPool;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::sleep;
use uuid::Uuid;

const NUM_CONSUMERS: u32 = 4;
const NUM_ROWS_LIMIT: u32 = 2;

async fn process_records(pool: MySqlPool, client: Client) {
    // let query_string = format!("SELECT id, source_estate_id, type_name, payload FROM processing_transactional_outbox WHERE is_processed = 0 LIMIT {}", NUM_CONSUMERS);
    let worker_uuid = Uuid::new_v4().to_string();
    let db_repository = DbRepository::new(pool).await;

    let log_data = log_hashmap! {
        "pkg_version" => env!("CARGO_PKG_VERSION"),
        "consumer_count" => NUM_CONSUMERS.to_owned(),
        "worker_uuid" => worker_uuid.to_owned()
    };

    debug!(target: "process_records", ctxt = log_data; "Starting a worker loop...");
    loop {
        //TODO: select records with filled worker_uuid without lock (reprocess failed)
        let worker_uuid = worker_uuid.clone();
        debug!(target: "process_records", ctxt = log_data; "Starting a transaction...");
        let mut tx = db_repository.start_transaction().await.unwrap_or_else(|_| {
            error!(target: "process_records", ctxt = log_data; "Failed to start a transaction");
            //TODO: continue here instead of panic
            panic!("worker {} failed to start a transaction", worker_uuid);
        });

        // // Set lock wait timeout
        // sqlx::query("SET innodb_lock_wait_timeout = 50")
        //     .execute(&mut tx)
        //     .await
        //     .unwrap();

        let records = DbRepository::lock_for_update(worker_uuid.clone(), &mut tx)
            .await
            .unwrap_or_else(|_| {
                error!(target: "process_records", ctxt = log_data; "Failed to lock for update");
                //TODO: continue here instead of panic
                panic!("worker {} failed to lock for update", worker_uuid);
            });

        info!(target: "process_records", ctxt = log_data; "Got {} records", records.len());

        // println!("Worker {} commits the transaction...", worker_uuid);
        // tx.commit().await.unwrap();
        // println!("Worker {} commited the transaction", worker_uuid);

        for record in records {
            let record_id = &String::from_utf8(record.id.clone()).unwrap();

            let res =
                DbRepository::update_worker_uuid(worker_uuid.clone(), record.id.clone(), &mut tx)
                    .await;
            let res = res.unwrap_or_else(|_| {
                error!(target: "process_records", ctxt = log_data; "Failed to update record {}", record_id);
                //TODO: continue here instead of panic
                panic!("Failed to update record {}", record_id)
            });

            if res.rows_affected() == 0 {
                warn!(target: "process_records", ctxt = log_data; "Has not set worker_uuid for record with id {}", record_id);
                continue;
            }

            debug!(target: "process_records", ctxt = log_data; "Set worker_uuid for record with id {}", record_id);
        }

        debug!(target: "process_records", ctxt = log_data; "Commiting the transaction...");
        tx.commit().await.unwrap();
        debug!(target: "process_records", ctxt = log_data; "Transaction commited");

        debug!(target: "process_records", ctxt = log_data; "Retrieving marked records...");
        let records = db_repository
            .worker_records(worker_uuid.clone())
            .await
            .unwrap_or_else(|_| {
                error!(target: "process_records", ctxt = log_data; "Failed to retrieve records");
                //TODO: continue here instead of panic
                panic!("worker {} failed to retrieve records", worker_uuid);
            });

        debug!(target: "process_records", ctxt = log_data; "Got {} records to process", records.len());

        for record in records {
            let res = client
                .post("https://httpbin.org/status/200")
                .json(&record)
                .send()
                .await;

            let record_id = &String::from_utf8(record.id.clone()).unwrap();
            if res.is_ok() {
                debug!(target: "process_records", ctxt = log_data; "The record with id has been processed {}", record_id);
                db_repository
                    .mark_as_processed(record.id.clone())
                    .await
                    .unwrap_or_else(|_| {
                        error!(target: "process_records", ctxt = log_data; "Failed to mark record as processed");
                        //TODO: continue here instead of panic
                        panic!(
                            "worker {} failed to mark record {} as processed",
                            worker_uuid, record_id
                        )
                    });
            } else {
                debug!(target: "process_records", ctxt = log_data; "Failed to process record with id {}", record_id);
            }
        }

        sleep(Duration::from_secs(1)).await;
    }
}

#[tokio::main]
async fn main() {
    init_logger();

    let mut log_data = log_hashmap! {
        "pkg_version" => env!("CARGO_PKG_VERSION"),
        "consumer_count" => NUM_CONSUMERS.to_owned()
    };

    log_data.insert("pkg_version", env!("CARGO_PKG_VERSION"));

    info!(target: "main", ctxt = log_data; "Starting the application...");
    let pool = PoolFactory::create_pool(2).await;

    if pool.is_err() {
        error!(target: "main", ctxt = log_data; "Failed to connect to the database");
        return;
    }

    let pool = pool.unwrap();
    let db_repository = DbRepository::new(pool.clone()).await;
    debug!(target: "main", ctxt = log_data; "Resetting worker_uuid field for unprocessed records...");
    //reset worker_uuid field for unprocessed records
    db_repository.reset_worker_ids().await;
    debug!(target: "main", ctxt = log_data; "worker_uuid field for unprocessed records has been reset");

    let client = Client::new();

    let mut handles = vec![];
    for _ in 0..NUM_CONSUMERS {
        let pool = pool.clone();
        let client = client.clone();
        handles.push(tokio::spawn(async move {
            process_records(pool, client).await;
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }
}
