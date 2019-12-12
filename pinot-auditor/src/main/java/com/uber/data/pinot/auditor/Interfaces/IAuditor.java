package com.uber.data.pinot.auditor.Interfaces;

public interface IAuditor {
    void track(
            long offset,
            long timestampInSec,
            int msgCount,
            int msgSizeInBytes);

    void flushAllMetrics();

    void flushTimeoutMetrics();
}
