package com.uber.data.pinot.auditor;

import com.uber.data.chaperone3.audit.TimeBucket;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class MessageAggregatorTest {
    @Test
    public void testBucketCountThreshold() {
        int timeBucketIntervalInSec = 600;

        int reportFreqBucketCount = 2;
        int reportFreqIntervalInMs = 3000;

        MessageAggregator ma =
                new MessageAggregator(
                        timeBucketIntervalInSec,
                        reportFreqBucketCount,
                        reportFreqIntervalInMs);

        long nowInSec = System.currentTimeMillis() / 1000;

        long offset = 1010;

        boolean readyToReport = ma.track(offset, nowInSec - timeBucketIntervalInSec - 10, 10, 1024);
        Assert.assertFalse(readyToReport);

        readyToReport = ma.track(offset + 1, nowInSec, 20, 1024);
        Assert.assertTrue(readyToReport);
        // timeout is not triggered
        Assert.assertFalse(ma.isTimeoutToReport());

        Map<Long, TimeBucket> map = ma.getTimeBucketsMap();

        for (TimeBucket tb : map.values()) {
            if (tb.getTotalCount() == 10) {
                // first bucket
                Assert.assertTrue(tb.startTimeSec <= nowInSec - timeBucketIntervalInSec - 10);
            } else if (tb.getTotalCount() == 20) {
                // second bucket
                Assert.assertTrue(tb.startTimeSec <= nowInSec);
            }
        }

        Assert.assertEquals(ma.getStartOffset(), offset);
        Assert.assertEquals(ma.getEndOffset(), offset + 1);
    }

    @Test
    public void testTimeoutThreshold() throws InterruptedException {
        int timeBucketIntervalInSec = 600;

        int reportFreqBucketCount = 2;
        int reportFreqIntervalInMs = 3000;

        MessageAggregator ma =
                new MessageAggregator(
                        timeBucketIntervalInSec,
                        reportFreqBucketCount,
                        reportFreqIntervalInMs);

        long nowInSec = System.currentTimeMillis() / 1000;

        long offset = 1010;

        boolean readyToReport = ma.track(offset, nowInSec, 10, 1024);
        Assert.assertFalse(readyToReport);

        // trigger timeout
        Thread.sleep(3000);
        Assert.assertTrue(ma.isTimeoutToReport());

        readyToReport = ma.track(offset + 1, nowInSec, 20, 1024);
        Assert.assertTrue(readyToReport);

        Map<Long, TimeBucket> map = ma.getTimeBucketsMap();
        Assert.assertEquals(map.size(), 1);

        for (TimeBucket tb : map.values()) {
            Assert.assertEquals(tb.getTotalCount(), 30);
        }

        Assert.assertEquals(ma.getStartOffset(), offset);
        Assert.assertEquals(ma.getEndOffset(), offset + 1);
    }
}
