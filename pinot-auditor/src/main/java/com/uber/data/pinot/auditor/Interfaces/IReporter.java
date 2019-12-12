package com.uber.data.pinot.auditor.Interfaces;

import com.uber.data.chaperone3.audit.TimeBucket;
import com.uber.data.pinot.auditor.Entities.MessageAggregatorIdentifier;

import java.util.Collection;

public interface IReporter {
    void report(MessageAggregatorIdentifier messageAggregatorIdentifier, Collection<TimeBucket> timeBucketList);

    void cleanup(long offset);

    void start();

    void shutdown();
}