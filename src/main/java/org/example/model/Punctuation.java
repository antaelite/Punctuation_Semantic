package org.example.model;

import lombok.Data;

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
