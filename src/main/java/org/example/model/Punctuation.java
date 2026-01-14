package org.example.model;

import java.io.Serial;

/**
 * Generic Punctuation. Semantics: "No more data for this `partitionKey`
 * regarding `field` up to `value` will arrive."
 */
public class Punctuation implements StreamElement {

    @Serial
    private static final long serialVersionUID = 1L;

    private String partitionKey; // Defines the stream partition (e.g. "NYC-TAXI" or sensor ID)
    private String field;        // The field being punctuated (e.g. "hour" or "id")
    private Object value;        // The value (inclusive) up to which data is complete
    private long timestamp;

    public Punctuation() {
    }

    public Punctuation(String partitionKey, String field, Object value, long timestamp) {
        this.partitionKey = partitionKey;
        this.field = field;
        this.value = value;
        this.timestamp = timestamp;
    }

    @Override
    public boolean isPunctuation() {
        return true;
    }

    @Override
    public String getKey() {
        return partitionKey;
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public Object getValue(String fieldName) {
        if (fieldName.equals(this.field)) {
            return value;
            // TODO: Handle type conversion if needed, but for now exact match.
        }
        return null;
    }

    // Getters / Setters for the punctuation specific info
    public String getField() {
        return field;
    }

    public Object getValue() {
        return value;
    }

    @Override
    public Object getDeduplicationKey() {
        return null;
    }

    @Override
    public String toString() {
        return "Punctuation{"
                + "key='" + partitionKey + '\''
                + ", field='" + field + '\''
                + ", value=" + value
                + ", ts=" + timestamp
                + '}';
    }
}
