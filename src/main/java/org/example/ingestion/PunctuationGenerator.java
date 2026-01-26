package org.example.ingestion;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.example.core.StreamItem;
import org.example.model.Punctuation;
import org.example.model.TaxiRide;

import java.time.Instant;

public class PunctuationGenerator implements FlatMapFunction<TaxiRide, StreamItem> {

    private String lastMedallion = null;
    private Long lastWindowStart = null;

    // 6 heures en ms
    private static final long WINDOW_SIZE = 21600000L;

    @Override
    public void flatMap(TaxiRide ride, Collector<StreamItem> out) throws Exception {
        if (ride == null) return;

        String currentMedallion = ride.medallion;
        long ts = ride.getPickupTimestamp();

        // Calcul de la tranche (00h, 06h, 12h, 18h...)
        long currentWindowStart = (ts / WINDOW_SIZE) * WINDOW_SIZE;

        // DEBUG: On affiche ce qui rentre
        // System.out.println("[GEN] REÇU: Taxi " + currentMedallion.substring(0, 5) + "... à " + ride.pickupDatetime + " (Fenêtre: " + Instant.ofEpochMilli(currentWindowStart) + ")");

        // 1. CHANGEMENT DE TAXI
        if (lastMedallion != null && !lastMedallion.equals(currentMedallion)) {
            System.out.println("\n[GEN] >>> CHANGEMENT TAXI détecté ! (" + lastMedallion + " -> " + currentMedallion + ")");
            System.out.println("[GEN] Emission Punctuation WILDCARD pour " + lastMedallion);

            out.collect(new Punctuation(lastMedallion));
            lastWindowStart = null;
        }

        // 2. CHANGEMENT DE FENÊTRE (00h->06h, etc.)
        else if (lastWindowStart != null && currentWindowStart > lastWindowStart) {
            long endOfPeriod = currentWindowStart - 1;

            System.out.println("\n[GEN] >>> SAUT DE FENÊTRE pour " + currentMedallion.substring(0, 8));
            System.out.println("[GEN] Ancienne: " + Instant.ofEpochMilli(lastWindowStart));
            System.out.println("[GEN] Nouvelle: " + Instant.ofEpochMilli(currentWindowStart));
            System.out.println("[GEN] Emission Punctuation: Fin de [" + Instant.ofEpochMilli(lastWindowStart) + " à " + Instant.ofEpochMilli(endOfPeriod) + "]");

            out.collect(new Punctuation(currentMedallion, lastWindowStart, endOfPeriod));
        }

        // 3. Émission Donnée
        out.collect(ride);

        lastMedallion = currentMedallion;
        lastWindowStart = currentWindowStart;
    }
}