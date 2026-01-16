package org.example.model;

import java.io.Serial;

/**
 * Generic Punctuation. Semantics: "No more data for this `partitionKey`
 * regarding `field` up to `value` will arrive."
 *
 * @param partitionKey Defines the stream partition (e.g. "NYC-TAXI" or sensor ID)
 * @param field        The field being punctuated (e.g. "hour" or "id")
 * @param value        The value (inclusive) up to which data is complete
 */
public record Punctuation(String partitionKey, String field, Object value, long timestamp) implements StreamElement {

    @Serial
    private static final long serialVersionUID = 1L;

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
    public Object getDeduplicationKey() {
        return null;
    }
}
