package com.uber.data.pinot.auditor.Entities;

import java.io.Serializable;

public class TimeBucketFlattenedMetric implements Serializable {
    private String tableName;
    private String region;
    private String topicName;
    private int partitionID;
    private int replicaID;

    // Not using offset fields for now
    private long startOffset;
    private long endOffset;

    private long startTimeSec;
    private long endTimeSec;
    private long totalCount;
    private long totalBytes;
    private long invalidCount;
    private double meanLatencyFromCreation;
    private double p99LatencyFromCreation;
    private double maxLatencyFromCreation;

    public TimeBucketFlattenedMetric(String tableName, String region, String topicName, int partitionID,
                                     int replicaID, long startOffset, long endOffset, long startTimeSec,
                                     long endTimeSec, long totalCount, long totalBytes, long invalidCount,
                                     double meanLatencyFromCreation, double p99LatencyFromCreation,
                                     double maxLatencyFromCreation) {
        this.tableName = tableName;
        this.region = region;
        this.topicName = topicName;
        this.partitionID = partitionID;
        this.replicaID = replicaID;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.startTimeSec = startTimeSec;
        this.endTimeSec = endTimeSec;
        this.totalCount = totalCount;
        this.totalBytes = totalBytes;
        this.invalidCount = invalidCount;
        this.meanLatencyFromCreation = meanLatencyFromCreation;
        this.p99LatencyFromCreation = p99LatencyFromCreation;
        this.maxLatencyFromCreation = maxLatencyFromCreation;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public int getPartitionID() {
        return partitionID;
    }

    public void setPartitionID(int partitionID) {
        this.partitionID = partitionID;
    }

    public int getReplicaID() {
        return replicaID;
    }

    public void setReplicaID(int replicaID) {
        this.replicaID = replicaID;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public void setStartOffset(long startOffset) {
        this.startOffset = startOffset;
    }

    public long getEndOffset() {
        return endOffset;
    }

    public void setEndOffset(long endOffset) {
        this.endOffset = endOffset;
    }

    public long getStartTimeSec() {
        return startTimeSec;
    }

    public void setStartTimeSec(long startTimeSec) {
        this.startTimeSec = startTimeSec;
    }

    public long getEndTimeSec() {
        return endTimeSec;
    }

    public void setEndTimeSec(long endTimeSec) {
        this.endTimeSec = endTimeSec;
    }

    public long getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(long totalCount) {
        this.totalCount = totalCount;
    }

    public long getTotalBytes() {
        return totalBytes;
    }

    public void setTotalBytes(long totalBytes) {
        this.totalBytes = totalBytes;
    }

    public long getInvalidCount() {
        return invalidCount;
    }

    public void setInvalidCount(long invalidCount) {
        this.invalidCount = invalidCount;
    }

    public double getMeanLatencyFromCreation() {
        return meanLatencyFromCreation;
    }

    public void setMeanLatencyFromCreation(double meanLatencyFromCreation) {
        this.meanLatencyFromCreation = meanLatencyFromCreation;
    }

    public double getP99LatencyFromCreation() {
        return p99LatencyFromCreation;
    }

    public void setP99LatencyFromCreation(double p99LatencyFromCreation) {
        this.p99LatencyFromCreation = p99LatencyFromCreation;
    }

    public double getMaxLatencyFromCreation() {
        return maxLatencyFromCreation;
    }

    public void setMaxLatencyFromCreation(double maxLatencyFromCreation) {
        this.maxLatencyFromCreation = maxLatencyFromCreation;
    }
}
