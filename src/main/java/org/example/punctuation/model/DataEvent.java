package org.example.punctuation.model;

public class DataEvent implements StreamElement {
    private String key;
    private String payload;
    private long timestamp;

    public DataEvent() {}

    public DataEvent(String key, String payload, long timestamp) {
        this.key = key;
        this.payload = payload;
        this.timestamp = timestamp;
    }

    @Override
    public boolean isPunctuation() {
        return false;
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    public String getPayload() {
        return payload;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "DataEvent{" +
                "key='" + key + '\'' +
                ", payload='" + payload + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
