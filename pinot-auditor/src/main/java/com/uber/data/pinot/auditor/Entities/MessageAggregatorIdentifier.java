package com.uber.data.pinot.auditor.Entities;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageAggregatorIdentifier {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private String tableName;
    private String region;
    private String topicName;
    private int partitionID;
    private int replicaID;

    public MessageAggregatorIdentifier(String tableName, String region, String topicName, int partitionID, int replicaID) {
        this.tableName = tableName;
        this.region = region;
        this.topicName = topicName;
        this.partitionID = partitionID;
        this.replicaID = replicaID;
    }

    @Override
    public String toString() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString(this);
        } catch (Exception e) {
            logger.error(
                    "Error in converting VirtualTablePayLoad object to JSON string : " + e.getMessage());
        }
        return null;
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
