package org.example.punctuation.model;

/**
 * Represents a Punctuation: a predicate stating that no more data
 * matching the definition will arrive.
 *
 * For this MVP, it specifically means "No more data for this Key".
 */
public class Punctuation implements StreamElement {
    private String targetKey;
    private long timestamp;

    public Punctuation() {}

    public Punctuation(String targetKey, long timestamp) {
        this.targetKey = targetKey;
        this.timestamp = timestamp;
    }

    @Override
    public boolean isPunctuation() {
        return true;
    }

    @Override
    public String getKey() {
        return targetKey;
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    public void setTargetKey(String targetKey) {
        this.targetKey = targetKey;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Punctuation{" +
                "targetKey='" + targetKey + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
