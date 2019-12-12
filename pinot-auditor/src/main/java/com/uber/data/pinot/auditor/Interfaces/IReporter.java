package com.uber.data.pinot.auditor.Interfaces;

import com.uber.data.chaperone3.audit.TimeBucket;
import com.uber.data.pinot.auditor.Entities.TimeBucketIdentifier;

import java.util.List;

public interface IReporter {
    void report(TimeBucketIdentifier timeBucketIdentifier, List<TimeBucket> timeBucketList);

    void cleanup(long offset);

    void start();

    void shutdown();
}