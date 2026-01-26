package org.example.model;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import lombok.Value;
import lombok.EqualsAndHashCode;

import org.example.core.StreamItem;

@Value
@EqualsAndHashCode(callSuper = false)
public class TaxiRide extends StreamItem {

    // Fields matching the CSV
    public String medallion;
    public String hackLicense;
    public String vendorId;
    public String pickupDatetime; // Keeping as String for simplicity in parsing
    public String dropoffDatetime;
    public double tripDistance;

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
