package org.example.model;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.example.core.StreamItem;

import java.time.Instant;

@Getter
@EqualsAndHashCode(callSuper = false)
public class Punctuation extends StreamItem {

    private final long start;
    private final long end;

    public Punctuation(long start, long end) {
        this.start = start;
        this.end = end;
    }

    @Override
    public boolean isPunctuation() {
        return true;
    }

    public boolean match(TaxiRide ride) {
        if (ride == null) return false;

        long rideTime = ride.getDropoffTimestamp();
        return rideTime >= this.start && rideTime <= this.end;
    }

    @Override
    public String toString() {
        return "Punctuation(start=" + Instant.ofEpochMilli(start)
                + ", end=" + Instant.ofEpochMilli(end) + ")";
    }
}
