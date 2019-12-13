package com.uber.data.pinot.auditor;

import com.uber.data.chaperone3.audit.TimeBucket;
import com.uber.data.pinot.auditor.Interfaces.IReporter;
import com.uber.data.pinot.auditor.PinotMessageAuditor.Builder;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Collection;

import static org.junit.Assert.assertEquals;

public class PinotMessageAuditorTest {
    @Test
    public void testBuilder() throws Exception {
        Builder builder = new Builder();
        PinotMessageAuditor auditor = null;
        try {
            auditor =
                    builder
                            .tableName("table")
                            .region("region")
                            .topicName("topic")
                            .partitionID(0)
                            .replicaID(0)
                            .timeBucketIntervalInSec(600)
                            .maxCountToReport(10)
                            .maxIntervalToReport(1000)
                            .build();
        } finally {
            if (auditor != null) {
                auditor.close();
            }
        }
    }

    @Test
    public void testFlushAllMetrics() {
        final int timeBucketIntervalInSec = 600;
        final int reportFreqBucketCount = 1024;
        final int reportFreqIntervalInMs = 10000;
        final String tableName = "table";
        final String region = "region";
        final String topicName = "topicName";
        final int partitionID = 0;
        final int replicaID = 0;

        IReporter reporter = Mockito.mock(IReporter.class);

        PinotMessageAuditor pma =
                new PinotMessageAuditor(
                        tableName,
                        region,
                        topicName,
                        partitionID,
                        replicaID,
                        reporter,
                        timeBucketIntervalInSec,
                        reportFreqBucketCount,
                        reportFreqIntervalInMs);

        long nowInSec =
                System.currentTimeMillis() / 1000 / timeBucketIntervalInSec * timeBucketIntervalInSec;

        pma.track(111, nowInSec - timeBucketIntervalInSec, 123, 123321);
        pma.track(333, nowInSec, 321, 321123);

        // force to generate audit metrics
        pma.flushAllMetrics();

        ArgumentCaptor<Collection<TimeBucket>> timeBuckets = ArgumentCaptor.forClass(Collection.class);
        Mockito.verify(reporter).report(timeBuckets.capture());
        assertEquals(2, timeBuckets.getValue().size());
    }

    //
    @Test
    public void testFlushTimeoutMetrics() throws Exception {
        final int timeBucketIntervalInSec = 600;
        final int reportFreqBucketCount = 1024;
        // for timeout to report
        final int reportFreqIntervalInMs = 10;
        final String tableName = "table";
        final String region = "region";
        final String topicName = "topicName";
        final int partitionID = 0;
        final int replicaID = 0;

        IReporter reporter = Mockito.mock(IReporter.class);

        PinotMessageAuditor pma =
                new PinotMessageAuditor(
                        tableName,
                        region,
                        topicName,
                        partitionID,
                        replicaID,
                        reporter,
                        timeBucketIntervalInSec,
                        reportFreqBucketCount,
                        reportFreqIntervalInMs);

        long nowInSec =
                System.currentTimeMillis() / 1000 / timeBucketIntervalInSec * timeBucketIntervalInSec;

        pma.track(111, nowInSec - timeBucketIntervalInSec, 123, 123321);
        pma.track(333, nowInSec, 321, 321123);

        Thread.sleep(100);
        pma.flushTimeoutMetrics();

        ArgumentCaptor<Collection<TimeBucket>> timeBuckets = ArgumentCaptor.forClass(Collection.class);
        Mockito.verify(reporter).report(timeBuckets.capture());
        assertEquals(2, timeBuckets.getValue().size());
    }

    @Test
    public void testTrackMsg() {
        final int timeBucketIntervalInSec = 600;
        // trigger report after counting 2 msg
        final int reportFreqBucketCount = 2;
        final int reportFreqIntervalInMs = 10000;
        final String tableName = "table";
        final String region = "region";
        final String topicName = "topicName";
        final int partitionID = 0;
        final int replicaID = 0;

        IReporter reporter = Mockito.mock(IReporter.class);

        PinotMessageAuditor pma =
                new PinotMessageAuditor(
                        tableName,
                        region,
                        topicName,
                        partitionID,
                        replicaID,
                        reporter,
                        timeBucketIntervalInSec,
                        reportFreqBucketCount,
                        reportFreqIntervalInMs);

        long nowInSec =
                System.currentTimeMillis() / 1000 / timeBucketIntervalInSec * timeBucketIntervalInSec;

        pma.track(111, nowInSec - timeBucketIntervalInSec, 123, 123321);
        pma.track(333, nowInSec, 321, 321123);

        ArgumentCaptor<Collection<TimeBucket>> timeBuckets = ArgumentCaptor.forClass(Collection.class);
        Mockito.verify(reporter).report(timeBuckets.capture());
        assertEquals(2, timeBuckets.getValue().size());
    }
}
