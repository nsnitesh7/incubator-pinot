package com.uber.data.pinot.auditor;

import com.google.common.base.Preconditions;
import com.uber.data.pinot.auditor.Entities.MessageAggregatorIdentifier;
import com.uber.data.pinot.auditor.Interfaces.IAuditor;
import com.uber.data.pinot.auditor.Interfaces.IReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

public class PinotMessageAuditor implements Closeable, IAuditor {
    private static final Logger logger = LoggerFactory.getLogger(PinotMessageAuditor.class);

    private final IReporter reporter;

    private final MessageAggregatorIdentifier messageAggregatorIdentifier;
    private final MessageAggregator messageAggregator;
    private final int timeBucketIntervalInSec;
    private final int reportFreqBucketCount;
    private final int reportFreqIntervalInMs;

    public PinotMessageAuditor(
            final String tableName,
            final String region,
            final String topicName,
            final int partitionID,
            final int replicaID,
            final IReporter reporter,
            final int timeBucketIntervalInSec,
            final int reportFreqBucketCount,
            final int reportFreqIntervalInMs) {
        this.timeBucketIntervalInSec = timeBucketIntervalInSec;
        this.reportFreqBucketCount = reportFreqBucketCount;
        this.reportFreqIntervalInMs = reportFreqIntervalInMs;
        this.reporter = reporter;
        this.messageAggregatorIdentifier = new MessageAggregatorIdentifier(tableName, region, topicName, partitionID, replicaID);
        this.messageAggregator = new MessageAggregator(timeBucketIntervalInSec, reportFreqBucketCount,
                reportFreqIntervalInMs);

    }

    @Override
    public void track(long offset, long timestampInSec, int msgCount, int msgSizeInBytes) {
        try {
            logger.debug("Audit msg from {}:{} at {}", messageAggregatorIdentifier.toString(), offset);

            boolean readyToReportAuditMsg =
                    messageAggregator.track(offset, timestampInSec, msgCount, msgSizeInBytes);

            logger.debug(
                    "Track msg of {} at {} with timestamp={}, msgCnt={}, msgSize={}",
                    messageAggregatorIdentifier.toString(),
                    offset,
                    timestampInSec,
                    msgCount,
                    msgSizeInBytes);

            if (readyToReportAuditMsg) {
                reporter.report(messageAggregatorIdentifier, messageAggregator.getTimeBuckets());
            }
        } catch (Exception e) {
            logger.warn("Got exception to audit msg for topic={}", messageAggregatorIdentifier.toString(), e);
        }
    }

    @Override
    public void flushAllMetrics() {
        logger.info("Flush all audit metrics");
        if (messageAggregator.hasTimeBuckets()) {
            reporter.report(messageAggregatorIdentifier, messageAggregator.getTimeBuckets());
        }
    }

    @Override
    public void flushTimeoutMetrics() {
        logger.info("Flush timeout audit metrics");
        if (messageAggregator.isTimeoutToReport()) {
            reporter.report(messageAggregatorIdentifier, messageAggregator.getTimeBuckets());
        }
    }

    @Override
    public void close() throws IOException {
        reporter.shutdown();
    }

    public static class Builder {

        private int timeBucketIntervalInSec = 60 * 10;
        private int reportFreqBucketCount = 4 * 1024 * 1000;
        private int reportFreqIntervalInMs = 2 * 60 * 1000;
        private String tableName;
        private String region;
        private String topicName;
        private int partitionID;
        private int replicaID;

        private int reportRequestTimeOutInMs = 60 * 1000;
        private int reportWaitOnShutdownInMs = 60 * 1000;
        private int reportMaxReportTaskQueueSize = 1024;

        public Builder timeBucketIntervalInSec(int timeBucketIntervalInSec) {
            this.timeBucketIntervalInSec = timeBucketIntervalInSec;
            return this;
        }

        public Builder maxCountToReport(int reportFreqBucketCount) {
            this.reportFreqBucketCount = reportFreqBucketCount;
            return this;
        }

        public Builder maxIntervalToReport(int reportFreqIntervalInMs) {
            this.reportFreqIntervalInMs = reportFreqIntervalInMs;
            return this;
        }

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder region(String region) {
            this.region = region;
            return this;
        }

        public Builder topicName(String topicName) {
            this.topicName = topicName;
            return this;
        }

        public Builder partitionID(int partitionID) {
            this.partitionID = partitionID;
            return this;
        }

        public Builder replicaID(int replicaID) {
            this.replicaID = replicaID;
            return this;
        }

        public Builder reportRequestTimeout(int reportRequestTimeOutInMs) {
            this.reportRequestTimeOutInMs = reportRequestTimeOutInMs;
            return this;
        }

        public Builder reporterShutdownTimeout(int reportWaitOnShutdownInMs) {
            this.reportWaitOnShutdownInMs = reportWaitOnShutdownInMs;
            return this;
        }

        public Builder maxReportTaskQueueSize(int reportMaxReportTaskQueueSize) {
            this.reportMaxReportTaskQueueSize = reportMaxReportTaskQueueSize;
            return this;
        }

        public PinotMessageAuditor build() {
            Preconditions.checkNotNull(tableName);
            Preconditions.checkNotNull(region);
            Preconditions.checkNotNull(topicName);
            Preconditions.checkNotNull(partitionID);
            Preconditions.checkNotNull(replicaID);
            MetricsReporter reporter =
                    new MetricsReporter(
                            reportRequestTimeOutInMs,
                            reportWaitOnShutdownInMs,
                            reportMaxReportTaskQueueSize);
            reporter.start();
            return new PinotMessageAuditor(tableName, region, topicName, partitionID, replicaID, reporter,
                    timeBucketIntervalInSec, reportFreqBucketCount, reportFreqIntervalInMs);
        }
    }
}
