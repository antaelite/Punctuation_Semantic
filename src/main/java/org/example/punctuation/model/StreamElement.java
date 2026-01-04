package org.example.punctuation.model;

/**
 * Common interface for all elements in the stream (Data and Punctuations).
 */
public interface StreamElement {
    boolean isPunctuation();
    long getTimestamp();
    String getKey(); // For MVP, we assume everything is keyed by a String
}
