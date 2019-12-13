package com.uber.data.pinot.auditor;

import com.uber.data.chaperone3.audit.TimeBucket;
import com.uber.data.pinot.auditor.Entities.MessageAggregatorIdentifier;
import com.uber.data.pinot.auditor.Entities.TimeBucketFlattenedMetric;
import com.uber.data.pinot.auditor.Interfaces.IReporter;
import com.uber.data.pinot.auditor.Storage.TimeBucketMetricsDAO;
import com.uber.data.pinot.auditor.Utils.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Collection;

public class MetricsReporter implements IReporter {
    private static final Logger logger = LoggerFactory.getLogger(MetricsReporter.class);

    private final TimeBucketMetricsDAO timeBucketMetricsDAO;
    private final MessageAggregatorIdentifier messageAggregatorIdentifier;

    public MetricsReporter(MessageAggregatorIdentifier messageAggregatorIdentifier) {
        timeBucketMetricsDAO = ConnectionFactory.getTimeBucketMetricsDao();
        this.messageAggregatorIdentifier = messageAggregatorIdentifier;
    }

    @Override
    public void report(Collection<TimeBucket> timeBucketList) {
        for (TimeBucket timeBucket : timeBucketList) {
            timeBucketMetricsDAO.writeMetric(new TimeBucketFlattenedMetric(
                    messageAggregatorIdentifier.getTableName(),
                    messageAggregatorIdentifier.getRegion(),
                    messageAggregatorIdentifier.getTopicName(),
                    messageAggregatorIdentifier.getPartitionID(),
                    messageAggregatorIdentifier.getReplicaID(),
                    0, 0,
                    timeBucket.startTimeSec * 1000,
                    timeBucket.endTimeSec * 1000,
                    timeBucket.getTotalCount(),
                    timeBucket.getTotalBytes(),
                    timeBucket.getInvalidCount(),
                    timeBucket.getMeanLatencyFromCreation(),
                    timeBucket.getP99LatencyFromCreation(),
                    timeBucket.getMaxLatencyFromCreation()));
        }
    }

    @Override
    public void cleanup(long offset) {
        throw new NotImplementedException();
    }
}
