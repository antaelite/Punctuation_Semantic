package org.example.model;

import java.io.Serializable;

public interface StreamElement implements Serializable {
    boolean isPunctuation();
//    boolean isData();
//    default Tuple asData() {
//        if (isData()) return (Tuple) this;
//        throw new ClassCastException("Element is not a Tuple");
//    }
//    default Punctuation asPunctuation() {
//        if (isPunctuation()) return (Punctuation) this;
//        throw new ClassCastException("Element is not a Punctuation");
//    }
}
