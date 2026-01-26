package org.example.model;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.example.core.StreamItem;

@Getter // Génère automatiquement tous les getX()
@ToString // Génère automatiquement le toString() joli
@EqualsAndHashCode(callSuper = false) // Génère equals() et hashCode() basés sur les champs
public class Punctuation extends StreamItem {

    private final String medallionPattern;
    private final long startTimestamp;
    private final long endTimestamp;

    // Constructeur 1 : Médaillon seul
    public Punctuation(String medallionPattern) {
        this(medallionPattern, -1, -1);
    }

    // Constructeur 2 : Temps seul
    public Punctuation(long start, long end) {
        this(null, start, end);
    }

    // Constructeur 3 : Complet (Médaillon + Temps)
    public Punctuation(String medallion, long start, long end) {
        this.medallionPattern = medallion;
        this.startTimestamp = start;
        this.endTimestamp = end;
    }

    @Override
    public boolean isPunctuation() {
        return true;
    }


    public boolean match(TaxiRide ride) {
        if (ride == null) return false;

        // 1. Vérification Médaillon
        if (this.medallionPattern != null) {
            if (!this.medallionPattern.equals(ride.medallion)) {
                return false;
            }
        }

        // 2. Vérification Temps
        if (this.startTimestamp != -1 && this.endTimestamp != -1) {
            long rideTime = ride.getPickupTimestamp();
            if (rideTime < this.startTimestamp || rideTime > this.endTimestamp) {
                return false;
            }
        }

        return true;
    }
}