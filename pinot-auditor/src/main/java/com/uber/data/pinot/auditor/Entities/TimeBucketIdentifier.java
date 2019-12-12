package com.uber.data.pinot.auditor.Entities;

public class TimeBucketIdentifier {
    private String tableName;
    private String region;
    private String topicName;
    private int partitionID;
    private int replicaID;

    public TimeBucketIdentifier(String tableName, String region, String topicName, int partitionID, int replicaID) {
        this.tableName = tableName;
        this.region = region;
        this.topicName = topicName;
        this.partitionID = partitionID;
        this.replicaID = replicaID;
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
}
