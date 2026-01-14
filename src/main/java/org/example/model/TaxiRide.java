package org.example.model;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Modèle de données pour un trajet de Taxi. Remplace SensorReading dans le
 * scénario Taxi.
 */
public class TaxiRide implements StreamElement {

    // Champs du CSV NYC Taxi
    public String rideId;
    private LocalDateTime pickupTime;

    // Formatter pour parser les dates du CSV (Ex: 2013-01-01 00:00:00)
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public TaxiRide() {
    }

    public TaxiRide(String csvLine) {
        String[] tokens = csvLine.split(",");

        this.rideId = tokens[0]; // Medallion comme ID unique du taxi
        this.pickupTime = LocalDateTime.parse(tokens[5], formatter);
    }

    // --- Implementation of StreamElement ---
    @Override
    public boolean isPunctuation() {
        return false;
    }

    @Override
    public long getTimestamp() {
        // Convertir LocalDateTime en millis pour Flink
        return java.sql.Timestamp.valueOf(pickupTime).getTime();
    }

    @Override
    public String getKey() {
        return "";
    }

    // --- Getters spécifiques ---
    @Override
    public Object getValue(String field) {
        if ("hour".equals(field)) {
            return pickupTime.getHour();
        } else if ("rideId".equals(field)) {
            return rideId;
        }
        return null;
    }

    @Override
    public Object getDeduplicationKey() {
        return rideId;
    }

    @Override
    public String toString() {
        return "TaxiRide{id=" + rideId + ", time=" + pickupTime + "}";
    }

    public int getHour() {
        return pickupTime.getHour();
    }
}
