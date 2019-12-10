CREATE DATABASE pinot;

USE pinot;

CREATE TABLE time_bucket_metrics (
    `table_name` char(100) NOT NULL DEFAULT '',
    `region` varchar(10) NOT NULL DEFAULT '',
    `topic_name` char(100) NOT NULL DEFAULT '',
    `partition_id` int DEFAULT 0,
    `replica_id` int DEFAULT 0,
    `start_offset` bigint DEFAULT 0,
    `end_offset` bigint DEFAULT 0,
    `start_time` bigint DEFAULT 0,
    `end_time` bigint DEFAULT 0,
    `total_count` bigint DEFAULT 0,
    `total_bytes` bigint DEFAULT 0,
    `invalid_count` bigint DEFAULT 0,
    `mean_latency` float DEFAULT 0,
    `p99_latency` float DEFAULT 0,
    `max_latency` float DEFAULT 0
);