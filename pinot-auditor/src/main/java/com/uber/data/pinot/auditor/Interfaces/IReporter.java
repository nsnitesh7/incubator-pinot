package com.uber.data.pinot.auditor.Interfaces;

import com.uber.data.chaperone3.audit.TimeBucket;

import java.util.Collection;

public interface IReporter {
    void report(Collection<TimeBucket> timeBucketList);

    void cleanup(long offset);
}