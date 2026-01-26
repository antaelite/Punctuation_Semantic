package org.example.model;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.example.core.StreamItem;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

@Getter
@ToString
@EqualsAndHashCode(callSuper = false)
public class Punctuation extends StreamItem {


    private final long startTimestamp;
    private final long endTimestamp;



    public Punctuation(long start, long end) {
        this.startTimestamp = start;
        this.endTimestamp = end;
    }

    @Override
    public boolean isPunctuation() {
        return true;
    }

    public boolean match(TaxiRide ride) {
        if (ride == null) return false;

        long rideTime = ride.getDropoffTimestamp();
        return rideTime >= this.startTimestamp && rideTime <= this.endTimestamp;
    }
    @Override
    public String toString() {
        return "Punctuation{Time:"
                + ", start=" + Instant.ofEpochMilli(startTimestamp)
                + ", end=" + Instant.ofEpochMilli(endTimestamp) + "}";
    }
}
