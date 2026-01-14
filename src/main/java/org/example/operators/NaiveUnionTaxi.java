package org.example.operators;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.model.StreamElement;
import org.example.model.TaxiRide;

/**
 * Opérateur Union Naïf.
 * Stocke tous les trajets vus pour dédoublonner, sans jamais nettoyer.
 * Problème : La mémoire (State) augmente linéairement à l'infini.
 */
public class NaiveUnionTaxi extends KeyedProcessFunction<String, StreamElement, String> {

    // MapState: Stocke <RideID, TaxiRide>
    // Dans le papier, c'est l'équivalent de la Hash Table qui grandit sans fin.
    private transient MapState<String, TaxiRide> seenRides;

    @Override
    public void open(Configuration parameters) {
        seenRides = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("naive-seen-rides", String.class, TaxiRide.class)
        );
    }

    @Override
    public void processElement(StreamElement element, Context ctx, Collector<String> out) throws Exception {

        if (element.isPunctuation()) {
            // Dans l'approche naïve, on IGNORE la ponctuation.
            // On ne fait aucun nettoyage.

            // On affiche juste la taille actuelle de la mémoire pour le graph
            int stateSize = 0;
            for (String key : seenRides.keys()) {
                stateSize++;
            }
            // Format pour la console : "NAIVE-UNION: [Taille]"
            System.out.println("NAIVE-UNION [" + ctx.getCurrentKey() + "]: State Size = " + stateSize);

        } else {
            // C'est un TaxiRide
            TaxiRide ride = (TaxiRide) element;

            // Logique UNION (Dédoublonnage)
            if (!seenRides.contains(ride.rideId)) {
                seenRides.put(ride.rideId, ride);
                // On pourrait émettre le trajet ici: out.collect(...)
            }
        }
    }
}