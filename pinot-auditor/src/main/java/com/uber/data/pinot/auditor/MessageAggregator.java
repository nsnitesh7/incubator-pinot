package com.uber.data.pinot.auditor;

import com.google.common.annotations.VisibleForTesting;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.uber.data.chaperone3.audit.TimeBucket;
import com.uber.data.pinot.auditor.Entities.TimeBucketIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Messages are aggregated together to generate a set of TimeBuckets. One aggregator is created per
 * topic-partition. Its commitOffset is also tracked here.
 */
public class MessageAggregator {
    private static final Logger logger = LoggerFactory.getLogger(MessageAggregator.class);

    private final TimeBucketIdentifier timeBucketIdentifier;
    private final int timeBucketIntervalInSec;

    // use concurrent version because the map will be iterated from other threads
    private Map<Long, TimeBucket> timeBucketsMap = new ConcurrentHashMap<>();

    // both boundaries are inclusive
    private long startOffset = -1;
    private long endOffset = -1;

    // threshold to control progress on emitting buckets
    private int timeBucketCount = 0;
    private final int reportFreqBucketCount;

    private long nextReportTimeInMs = 0L;
    private final int reportFreqIntervalInMs;

    public MessageAggregator(
            final TimeBucketIdentifier timeBucketIdentifier,
            final int timeBucketIntervalInSec,
            final int reportFreqBucketCount,
            final int reportFreqIntervalInMs) {
        this.timeBucketIdentifier = timeBucketIdentifier;
        this.timeBucketIntervalInSec = timeBucketIntervalInSec;

        this.reportFreqBucketCount = reportFreqBucketCount;
        this.reportFreqIntervalInMs = reportFreqIntervalInMs;

        resetAggregator();
    }

    public String getTopicName() {
        return timeBucketIdentifier.getTopicName();
    }

    public int getPartitionID() {
        return timeBucketIdentifier.getPartitionID();
    }

    private void resetAggregator() {
        timeBucketsMap = new ConcurrentHashMap<>();
        startOffset = -1;
        // endOffset is kept increasing
        timeBucketCount = 0;
        nextReportTimeInMs = System.currentTimeMillis() + reportFreqIntervalInMs;
    }

    private long getBucketBeginSec(long timestampInSec) {
        return timestampInSec - (timestampInSec % timeBucketIntervalInSec);
    }

    private TimeBucket getTimeBucket(final long timestampInSec) {
        long bucketBeginSec = getBucketBeginSec(timestampInSec);

        TimeBucket timeBucket = timeBucketsMap.get(bucketBeginSec);
        if (timeBucket == null) {
            timeBucket = new TimeBucket(bucketBeginSec, bucketBeginSec + timeBucketIntervalInSec);
            timeBucketsMap.put(bucketBeginSec, timeBucket);
            timeBucketCount += 1;
        }
        return timeBucket;
    }

    public boolean track(long offset, long timestampInSec, int msgCount, int msgSizeInBytes) {
        // update offsets
        endOffset = Math.max(endOffset, offset);
        if (startOffset == -1) {
            startOffset = offset;
        }

        // update time bucket statistics
        TimeBucket timeBucket = getTimeBucket(timestampInSec);
        timeBucket.trackTotalCount(msgCount);
        timeBucket.trackTotalBytes(msgSizeInBytes);

        long currentTimeInMs = System.currentTimeMillis();

        double creationLatency = currentTimeInMs - (timestampInSec * 1000);
        for (int i = 0; i < msgCount; i++) {
            timeBucket.trackLatencyFromCreation(creationLatency);
        }

        // decide if time to report buckets
        if (timeBucketCount >= reportFreqBucketCount) {
            logger.debug(
                    "FreqBucketCount is reached freqBucketCount={}, bucketCount={}, timeBucketIdentifier={}",
                    reportFreqBucketCount,
                    timeBucketCount,
                    timeBucketIdentifier.toString());
        }

        if (currentTimeInMs >= nextReportTimeInMs) {
            logger.debug(
                    "FreqTimeInterval is reached currentTimeInMs={}, nextTimeInMs={}, timeBucketIdentifier={}",
                    currentTimeInMs,
                    nextReportTimeInMs,
                    timeBucketIdentifier.toString());
        }

        return timeBucketCount >= reportFreqBucketCount || currentTimeInMs >= nextReportTimeInMs;
    }

    public boolean isTimeoutToReport() {
        return timeBucketCount > 0 && System.currentTimeMillis() >= nextReportTimeInMs;
    }

    public boolean hasTimeBuckets() {
        return timeBucketCount > 0;
    }

    public void markInvalidMsg(final int msgCount, final long nowInSec) {
        TimeBucket timeBucket = getTimeBucket(nowInSec);
        timeBucket.trackInvalidCount(msgCount);
    }

    public Collection<TimeBucket> getTimeBuckets(
            final String auditHost, final String auditTier, boolean resetAggregator) {
        Collection<TimeBucket> timeBuckets = timeBucketsMap.values();
        resetAggregator();
        return timeBuckets;
    }

    @VisibleForTesting
    public Map<Long, TimeBucket> getTimeBucketsMap() {
        return timeBucketsMap;
    }

    @VisibleForTesting
    public long getStartOffset() {
        return startOffset;
    }

    @VisibleForTesting
    public long getEndOffset() {
        return endOffset;
    }
}
