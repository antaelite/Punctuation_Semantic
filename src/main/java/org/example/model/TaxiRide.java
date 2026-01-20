package org.example.model;

import java.io.Serial;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.apache.commons.lang3.math.NumberUtils;

/**
 * NYC Taxi ride data model with full pickup and drop-off information. Supports
 * the Tucker et al. 2003 wait-time-between-fares query.
 *
 * <p>
 * <b>Serialization:</b> Uses primitive types (long timestamps) for Flink/Kryo
 * compatibility.
 */
public class TaxiRide implements StreamElement {

    @Serial
    private static final long serialVersionUID = 1L;

    // CSV fields (using timestamps for serialization compatibility)
    private final String medallion;           // Taxi ID (column 0)
    private final long pickupTimestamp;       // Column 5 (as epoch millis)
    private final long dropoffTimestamp;      // Column 6 (as epoch millis)
    private final double dropoffLongitude;    // Column 13
    private final double dropoffLatitude;     // Column 14

    private static final transient DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * Parse CSV line into TaxiRide object. CSV format:
     * medallion,hack_license,vendor_id,rate_code,store_and_fwd_flag,
     * pickup_datetime,dropoff_datetime,passenger_count,trip_time_in_secs,
     * trip_distance,pickup_longitude,pickup_latitude,dropoff_longitude,dropoff_latitude
     */
    public TaxiRide(String csvLine) {
        String[] tokens = csvLine.split(",");

        this.medallion = tokens[0];
        LocalDateTime pickupTime = LocalDateTime.parse(tokens[5], formatter);
        LocalDateTime dropoffTime = LocalDateTime.parse(tokens[6], formatter);
        this.pickupTimestamp = java.sql.Timestamp.valueOf(pickupTime).getTime();
        this.dropoffTimestamp = java.sql.Timestamp.valueOf(dropoffTime).getTime();

        // Parse coordinates (maybe 0.0 for missing data)
        this.dropoffLongitude = NumberUtils.toDouble(tokens[12], 0.0);
        this.dropoffLatitude = NumberUtils.toDouble(tokens[13], 0.0);
    }

    // StreamElement implementation
    @Override
    public boolean isPunctuation() {
        return false;
    }

    @Override
    public long timestamp() {
        return getDropoffTimestamp(); // Event time = when taxi reports data (dropoff)
    }

    @Override
    public String getKey() {
        return medallion; // Key by taxi ID for query
    }

    @Override
    public Object getValue(String field) {
        return switch (field) {
            case "pickup_hour" ->
                LocalDateTime.ofInstant(
                java.time.Instant.ofEpochMilli(pickupTimestamp),
                java.time.ZoneId.systemDefault()
                ).getHour();
            case "medallion" ->
                medallion;
            case "dropoff_hour" ->
                LocalDateTime.ofInstant(
                java.time.Instant.ofEpochMilli(dropoffTimestamp),
                java.time.ZoneId.systemDefault()
                ).getHour();
            default ->
                null;
        };
    }

    public long getPickupTimestamp() {
        return pickupTimestamp;
    }

    public long getDropoffTimestamp() {
        return dropoffTimestamp;
    }

    /**
     * Determine destination borough from dropoff coordinates. Simplified
     * bounding box approach for NYC boroughs.
     */
    public String getDropoffBorough() {
        if (dropoffLongitude == 0.0 && dropoffLatitude == 0.0) {
            return "Unknown";
        }

        // Simplified NYC borough boundaries (approximate)
        // Manhattan: -74.02 to -73.91 longitude, 40.70 to 40.88 latitude
        if (dropoffLongitude >= -74.02 && dropoffLongitude <= -73.91
                && dropoffLatitude >= 40.70 && dropoffLatitude <= 40.88) {
            return "Manhattan";
        }

        // Brooklyn: -74.05 to -73.83 longitude, 40.57 to 40.74 latitude
        if (dropoffLongitude >= -74.05 && dropoffLongitude <= -73.83
                && dropoffLatitude >= 40.57 && dropoffLatitude <= 40.74) {
            return "Brooklyn";
        }

        // Queens: -73.96 to -73.70 longitude, 40.54 to 40.80 latitude
        if (dropoffLongitude >= -73.96 && dropoffLongitude <= -73.70
                && dropoffLatitude >= 40.54 && dropoffLatitude <= 40.80) {
            return "Queens";
        }

        // Bronx: -73.93 to -73.75 longitude, 40.79 to 40.92 latitude
        if (dropoffLongitude >= -73.93 && dropoffLongitude <= -73.75
                && dropoffLatitude >= 40.79 && dropoffLatitude <= 40.92) {
            return "Bronx";
        }

        // Staten Island: -74.26 to -74.05 longitude, 40.49 to 40.65 latitude
        if (dropoffLongitude >= -74.26 && dropoffLongitude <= -74.05
                && dropoffLatitude >= 40.49 && dropoffLatitude <= 40.65) {
            return "StatenIsland";
        }

        return "Other";
    }

    @Override
    public String toString() {
        LocalDateTime pickup = LocalDateTime.ofInstant(
                java.time.Instant.ofEpochMilli(pickupTimestamp),
                java.time.ZoneId.systemDefault()
        );
        LocalDateTime dropoff = LocalDateTime.ofInstant(
                java.time.Instant.ofEpochMilli(dropoffTimestamp),
                java.time.ZoneId.systemDefault()
        );
        return "TaxiRide{medallion=" + medallion
                + ", pickup=" + pickup
                + ", dropoff=" + dropoff
                + ", borough=" + getDropoffBorough() + "}";
    }
}
