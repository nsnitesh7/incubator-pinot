package com.uber.data.pinot.auditor;

public final class Constants {
    public static final String DATABASE_URL = "jdbc:mysql://localhost:3306/pinot";
    public static final String USER = "root";
    public static final String PASS = "pass";

    public static final String tableName = "table_name";
    public static final String region = "region";
    public static final String topicName = "topic_name";
    public static final String partitionID = "partition_id";
    public static final String replicaID = "replica_id";
    public static final String startOffset = "start_offset";
    public static final String endOffset = "end_offset";
    public static final String startTimeSec = "start_time";
    public static final String endTimeSec = "end_time";
    public static final String totalCount = "total_count";
    public static final String totalBytes = "total_bytes";
    public static final String invalidCount = "invalid_count";
    public static final String meanLatencyFromCreation = "mean_latency";
    public static final String p99LatencyFromCreation = "p99_latency";
    public static final String maxLatencyFromCreation = "max_latency";
}
