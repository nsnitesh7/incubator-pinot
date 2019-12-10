package com.uber.data.pinot.auditor.Storage;

import com.uber.data.pinot.auditor.Entities.TimeBucketFlattenedMetric;
import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.util.List;

public interface TimeBucketMetricsDAO {

    @SqlUpdate("INSERT INTO time_bucket_metrics(" +
            "table_name, region, topic_name, partition_id, replica_id, start_offset, end_offset, start_time, " +
            "end_time, total_count, total_bytes, invalid_count, mean_latency, p99_latency, " +
            "max_latency) VALUES(" +
            ":tableName, :region, :topicName, :partitionID, :replicaID, :startOffset, :endOffset, :startTimeSec, " +
            ":endTimeSec, :totalCount, :totalBytes, :invalidCount, :meanLatencyFromCreation, :p99LatencyFromCreation, " +
            ":maxLatencyFromCreation)")
    void writeMetric(@BindBean TimeBucketFlattenedMetric metric);

    @SqlQuery("SELECT * from time_bucket_metrics")
    @RegisterRowMapper(TimeBucketFlattenedMetricMapper.class)
    List<TimeBucketFlattenedMetric> getMetrics();
}
