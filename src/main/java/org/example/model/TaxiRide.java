package org.example.model;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Modèle de données pour un trajet de Taxi.
 * Remplace SensorReading dans le scénario Taxi.
 */
public class TaxiRide implements StreamElement, Serializable {

    // Champs du CSV NYC Taxi
    public String rideId;
    private LocalDateTime pickupTime;
    private LocalDateTime dropoffTime;
    private double pickupLon;
    private double pickupLat;
    private double dropoffLon;
    private double dropoffLat;

    // Formatter pour parser les dates du CSV (Ex: 2013-01-01 00:00:00)
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public TaxiRide() {}

    public TaxiRide(String csvLine) {
        String[] tokens = csvLine.split(",");

        this.rideId = tokens[0]; // Medallion comme ID unique du taxi
        this.pickupTime = LocalDateTime.parse(tokens[5], formatter);
        this.dropoffTime = LocalDateTime.parse(tokens[6], formatter);

        try {
            this.pickupLon = Double.parseDouble(tokens[10]);
            this.pickupLat = Double.parseDouble(tokens[11]);
            this.dropoffLon = Double.parseDouble(tokens[12]);
            this.dropoffLat = Double.parseDouble(tokens[13]);
        } catch (Exception e) {
            // Gérer les coordonnées manquantes
            this.pickupLon = 0.0;
            this.pickupLat = 0.0;
        }
    }

    // --- Implémentation de StreamElement ---

    @Override
    public boolean isPunctuation() {
        return false;
    }

    @Override
    public String getKeyTaxi() {
        return "NYC-TAXI";
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

    public int getHour() {
        return pickupTime.getHour();
    }

    @Override
    public String toString() {
        return "TaxiRide{id=" + rideId + ", time=" + pickupTime + "}";
    }
}