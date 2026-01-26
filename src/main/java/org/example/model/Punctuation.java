package org.example.model;

import org.example.core.StreamItem;

public class Punctuation extends StreamItem {

    @Override
    public boolean isPunctuation() {
        return true;
    }

    public boolean match(TaxiRide ride) {
        // TODO
        return false;
    }
}
