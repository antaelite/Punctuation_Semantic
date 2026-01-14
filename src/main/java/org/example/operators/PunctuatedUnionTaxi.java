package org.example.operators;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.model.HourlyPunctuation;
import org.example.model.StreamElement;
import org.example.model.TaxiRide;

import java.util.Iterator;
import java.util.Map;

/**
 * Opérateur Union Optimisé (Punctuated).
 * Utilise la sémantique de ponctuation pour nettoyer l'état.
 * Résultat : La mémoire forme une dent de scie (Sawtooth).
 */
public class PunctuatedUnionTaxi extends KeyedProcessFunction<String, StreamElement, String> {

    // On stocke les trajets, mais on va les nettoyer.
    private transient MapState<String, TaxiRide> seenRides;

    @Override
    public void open(Configuration parameters) {
        seenRides = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("punctuated-seen-rides", String.class, TaxiRide.class)
        );
    }

    @Override
    public void processElement(StreamElement element, Context ctx, Collector<String> out) throws Exception {

        if (element.isPunctuation()) {
            // --- C'EST UNE PONCTUATION ---
            HourlyPunctuation punc = (HourlyPunctuation) element;
            int hourToPurge = punc.getHour();

            System.out.println(">>> PUNCTUATED-UNION: Received End-of-Hour " + hourToPurge + ". Cleaning state...");

            // KEEP INVARIANT : On nettoie l'état correspondant à la ponctuation [cite: 310]
            // On parcourt l'état et on supprime tout ce qui appartient à l'heure finie.
            Iterator<Map.Entry<String, TaxiRide>> iterator = seenRides.iterator();
            int deletedCount = 0;

            while (iterator.hasNext()) {
                Map.Entry<String, TaxiRide> entry = iterator.next();
                TaxiRide ride = entry.getValue();

                if (ride.getHour() <= hourToPurge) {
                    iterator.remove(); // Suppression efficace via l'itérateur Flink
                    deletedCount++;
                }
            }

            // Métrique pour vérifier que ça marche
            int remainingSize = 0;
            for (String key : seenRides.keys()) remainingSize++;

            System.out.println("PUNCTUATED-UNION: Purged " + deletedCount + " rides. New State Size = " + remainingSize);

        } else {
            // --- C'EST UN TAXI ---
            TaxiRide ride = (TaxiRide) element;

            // Logique UNION (Dédoublonnage standard)
            if (!seenRides.contains(ride.rideId)) {
                seenRides.put(ride.rideId, ride);
            }
        }
    }
}