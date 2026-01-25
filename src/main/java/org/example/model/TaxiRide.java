package org.example.model;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import lombok.Data;

public class TaxiRide extends StreamItem {

    // Fields matching the CSV
    public String medallion;
    public String hackLicense;
    public String vendorId;
    public String pickupDatetime; // Keeping as String for simplicity in parsing
    public String dropoffDatetime;
    public double tripDistance;

    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public TaxiRide(String medallion, String hackLicense, String vendorId,
            String pickupDatetime, String dropoffDatetime, double tripDistance) {
        this.medallion = medallion;
        this.hackLicense = hackLicense;
        this.vendorId = vendorId;
        this.pickupDatetime = pickupDatetime;
        this.dropoffDatetime = dropoffDatetime;
        this.tripDistance = tripDistance;
    }

    public long getPickupTimestamp() {
        return LocalDateTime.parse(pickupDatetime, formatter)
                .toInstant(java.time.ZoneOffset.UTC).toEpochMilli();
    }

    @Override
    public boolean isPunctuation() {
        return false;
    }

    @Override
    public String toString() {
        return String.format("Ride{medallion='%s', pickup='%s'}", medallion, pickupDatetime);
    }

}
