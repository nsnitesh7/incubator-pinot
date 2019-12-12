package com.uber.data.pinot.auditor.Storage;

import com.uber.data.pinot.auditor.Utils.Constants;
import com.uber.data.pinot.auditor.Entities.TimeBucketFlattenedMetric;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;

import java.sql.ResultSet;
import java.sql.SQLException;

public class TimeBucketFlattenedMetricMapper implements RowMapper<TimeBucketFlattenedMetric> {

    @Override
    public TimeBucketFlattenedMetric map(ResultSet rs, StatementContext ctx) throws SQLException {
        TimeBucketFlattenedMetric metric =
                new TimeBucketFlattenedMetric(
                        rs.getString(Constants.tableName),
                        rs.getString(Constants.region),
                        rs.getString(Constants.topicName),
                        rs.getInt(Constants.partitionID),
                        rs.getInt(Constants.replicaID),
                        rs.getLong(Constants.startOffset),
                        rs.getLong(Constants.endOffset),
                        rs.getLong(Constants.startTimeSec),
                        rs.getLong(Constants.endTimeSec),
                        rs.getLong(Constants.totalCount),
                        rs.getLong(Constants.totalBytes),
                        rs.getLong(Constants.invalidCount),
                        rs.getDouble(Constants.meanLatencyFromCreation),
                        rs.getDouble(Constants.p99LatencyFromCreation),
                        rs.getDouble(Constants.maxLatencyFromCreation));
        return metric;
    }
}
