package org.apache.flink.research.punctuation.core;

import java.io.Serializable;
import java.util.Objects;

/**
 * A generic wrapper representing either a Data tuple or a Punctuation.
 * This directly implements the theoretical definition of S in Tucker/Maier (2003),
 * where the stream is a union of Data (D) and Punctuations (P).
 *
 * @param <T> The type of the data tuple (The payload).
 * @param <K> The type of the punctuation key (The predicate target).
 */
public class PunctuatedRecord<T, K> implements Serializable {

    public enum Type {
        DATA,
        PUNCTUATION
    }

    private Type type;
    private T data;
    private K punctuationKey;
    private long timestamp; // Explicit timestamp for Flink event time integration

    // Default constructor for Flink POJO serialization compliance
    public PunctuatedRecord() {}

    /**
     * Creates a Data record.
     * @param data The payload.
     * @param timestamp The event time.
     */
    public PunctuatedRecord(T data, long timestamp) {
        this.type = Type.DATA;
        this.data = data;
        this.punctuationKey = null;
        this.timestamp = timestamp;
    }

    /**
     * Creates a Punctuation record.
     * Corresponds to a predicate: "No more data for key K".
     * @param punctuationKey The key for which the stream is closed.
     * @param timestamp The timestamp of the punctuation.
     */
    public PunctuatedRecord(K punctuationKey, long timestamp) {
        this.type = Type.PUNCTUATION;
        this.data = null;
        this.punctuationKey = punctuationKey;
        this.timestamp = timestamp;
    }

    public boolean isPunctuation() {
        return type == Type.PUNCTUATION;
    }

    public boolean isData() {
        return type == Type.DATA;
    }

    public T getData() {
        return data;
    }

    public K getPunctuationKey() {
        return punctuationKey;
    }
    
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return isData()? "Data(" + data + ")" : "Punctuation(End:" + punctuationKey + ")";
    }
}