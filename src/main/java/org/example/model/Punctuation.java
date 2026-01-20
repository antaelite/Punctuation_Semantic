package org.example.model;

import java.io.Serial;

/**
 * Generic Punctuation. Semantics: "No more data for this `partitionKey`
 * regarding `field` up to `value` will arrive."
 *
 * <p>
 * <b>Tucker et al. 2003 Concept:</b> Punctuations are special stream elements
 * that mark completeness boundaries, enabling bounded state in operators.
 *
 * <p>
 * <b>Note:</b> Converted from record to POJO for Kryo serialization
 * compatibility.
 */
public class Punctuation implements StreamElement {

    @Serial
    private static final long serialVersionUID = 1L;

    private final String partitionKey;
    private final String field;
    private final Object value;
    private final long timestamp;

    /**
     * No-arg constructor required for Kryo serialization.
     */
    public Punctuation() {
        this.partitionKey = "";
        this.field = "";
        this.value = null;
        this.timestamp = 0L;
    }

    /**
     * Create a punctuation marker.
     *
     * @param partitionKey Defines the stream partition (e.g. "NYC-TAXI" or
     * sensor ID)
     * @param field The field being punctuated (e.g. "hour" or "id")
     * @param value The value (inclusive) up to which data is complete
     * @param timestamp The timestamp of the punctuation
     */
    public Punctuation(String partitionKey, String field, Object value, long timestamp) {
        this.partitionKey = partitionKey;
        this.field = field;
        this.value = value;
        this.timestamp = timestamp;
    }

    public String partitionKey() {
        return partitionKey;
    }

    public String field() {
        return field;
    }

    public Object value() {
        return value;
    }

    @Override
    public long timestamp() {
        return timestamp;
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
    public Object getValue(String fieldName) {
        return fieldName.equals(field) ? value : null;
    }

    @Override
    public String toString() {
        return "Punctuation{partitionKey='" + partitionKey + "', field='" + field
                + "', value=" + value + ", timestamp=" + timestamp + "}";
    }
}
