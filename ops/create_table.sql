create database transactional_outbox;
drop table if exists processing_transactional_outbox;
CREATE TABLE processing_transactional_outbox
(
    id               VARBINARY(36)   NOT NULL,
    source_estate_id BIGINT UNSIGNED NOT NULL,
    type_name        VARCHAR(35)     NOT NULL,
    payload          TEXT                     DEFAULT NULL,
    worker_uuid      VARBINARY(36)            DEFAULT NULL,
    created_at       TIMESTAMP(3)             default CURRENT_TIMESTAMP(3) not null,
    is_processed     TINYINT(1)      NOT NULL DEFAULT 0,
    PRIMARY KEY (id)
);

create index idx_created_at on processing_transactional_outbox (created_at);
create index idx_is_processed on processing_transactional_outbox (is_processed);
