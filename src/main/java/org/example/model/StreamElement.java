package org.example.model;

import java.io.Serializable;

/**
 * Common interface for all elements in the stream (Data and Punctuations).
 */
public interface StreamElement extends Serializable {

    boolean isPunctuation();

    long timestamp();

    String getKey();

    Object getValue(String field);
}
