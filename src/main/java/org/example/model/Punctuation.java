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

    private final String medallion;
    private final long startTimestamp;
    private final long endTimestamp;

    public Punctuation(String medallion) {
        this(medallion, -1, -1);
    }

    public Punctuation(String medallion, long start, long end) {
        this.medallion = medallion;
        this.startTimestamp = start;
        this.endTimestamp = end;
    }

    @Override
    public boolean isPunctuation() {
        return true;
    }

    public boolean match(TaxiRide ride) {
        if (ride == null || !Objects.equals(this.medallion, ride.medallion)) {
            return false;
        }

        if (this.startTimestamp != -1 && this.endTimestamp != -1) {
            long rideTime = ride.getPickupTimestamp();
            return rideTime >= this.startTimestamp && rideTime <= this.endTimestamp;
        }
        return true;
    }

    @Override
    public String toString() {
        if (startTimestamp == -1 || endTimestamp == -1) {
            return "Punctuation{medallion=" + medallion + ", type=WILDCARD}";
        }
        return "Punctuation{medallion=" + medallion
                + ", start=" + Instant.ofEpochMilli(startTimestamp)
                + ", end=" + Instant.ofEpochMilli(endTimestamp) + "}";
    }
}
