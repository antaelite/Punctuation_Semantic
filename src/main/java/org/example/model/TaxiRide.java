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
    public int passengerCount;
    public double tripDistance;
    public String pickupLongitude;
    public String pickupLatitude;
    public String dropoffLongitude;
    public String dropoffLatitude;

    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public long getDropoffTimestamp() {
        return LocalDateTime.parse(dropoffDatetime, formatter)
                .toInstant(java.time.ZoneOffset.UTC).toEpochMilli();
    }

    @Override
    public boolean isPunctuation() {
        return false;
    }

}
