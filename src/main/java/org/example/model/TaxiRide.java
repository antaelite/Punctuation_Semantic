package org.example.model;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.example.core.StreamItem;

@Data
@EqualsAndHashCode(callSuper = false)
public class TaxiRide extends StreamItem {

    // Fields matching the CSV
    public final String medallion;
    public final String hackLicense;
    public final String vendorId;
    public final String pickupDatetime; // Keeping as String for simplicity in parsing
    public final String dropoffDatetime;
    public final double tripDistance;

    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public long getPickupTimestamp() {
        return LocalDateTime.parse(pickupDatetime, formatter)
                .toInstant(java.time.ZoneOffset.UTC).toEpochMilli();
    }

    @Override
    public boolean isPunctuation() {
        return false;
    }

}
