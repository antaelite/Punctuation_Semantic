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
    public final String medallion;
    public final String hackLicense;
    public final String vendorId;
    public final String pickupDatetime; // Keeping as String for simplicity in parsing
    public final String dropoffDatetime;
    public final double tripDistance;
    public final String pickupLongitude;
    public final String pickupLatitude;
    public final String dropoffLongitude;
    public final String dropoffLatitude;

    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public long getPickupTimestamp() {
        return LocalDateTime.parse(pickupDatetime, formatter)
                .toInstant(java.time.ZoneOffset.UTC).toEpochMilli();
    }

    public long getDropoffTimestamp() {
        return LocalDateTime.parse(dropoffDatetime, formatter)
                .toInstant(java.time.ZoneOffset.UTC).toEpochMilli();
    }

    @Override
    public boolean isPunctuation() {
        return false;
    }

}
