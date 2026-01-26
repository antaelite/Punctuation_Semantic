package org.example.operators;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

// Attention aux imports selon votre structure de dossiers
import org.example.core.PunctuatedIterator;
import org.example.core.StreamItem;
import org.example.model.Punctuation;
import org.example.model.TaxiRide;

import java.util.Iterator;
import java.util.Map;

public class StreamGroupBy extends PunctuatedIterator {

    // On stocke l'état : Médaillon -> Distance Totale
    private MapState<String, Double> runningSums;

    @Override
    public void open(Configuration parameters) {
        // Initialisation de la mémoire Flink
        runningSums = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("runningSums", String.class, Double.class)
        );
    }

    /**
     * STEP : Accumulation (On ne sort rien ici !)
     * On reçoit un trajet, on ajoute sa distance au total en mémoire.
     */
    @Override
    public void step(TaxiRide ride, Context context, Collector<StreamItem> out) throws Exception {
        Double currentTotal = runningSums.get(ride.medallion);

        if (currentTotal == null) {
            currentTotal = 0.0;
        }

        // On met à jour la mémoire (State)
        runningSums.put(ride.medallion, currentTotal + ride.tripDistance);

        // Notez qu'on ne fait PAS de out.collect() ici.
        // On attend la fin de la période (la ponctuation).
    }

    /**
     * PASS : Émission des résultats
     * La ponctuation arrive : "La tranche 00h-06h est finie".
     * On regarde ce qu'on a calculé et on l'envoie.
     */
    @Override
    public void pass(Punctuation p, Context context, Collector<StreamItem> out) throws Exception {
        Iterator<Map.Entry<String, Double>> iterator = runningSums.iterator();

        while (iterator.hasNext()) {
            Map.Entry<String, Double> entry = iterator.next();
            String medallionInState = entry.getKey();
            Double totalDistance = entry.getValue();

            // Vérification : Est-ce que la ponctuation concerne ce médaillon ?
            // (Soit c'est le même médaillon, soit c'est null/wildcard qui veut dire "tous")
            String pattern = p.getMedallion();
            boolean match = (pattern == null) || pattern.equals(medallionInState);

            if (match) {
                // Création d'un objet résultat (On réutilise TaxiRide pour simplifier)
                // On met "TOTAL_RESULT" pour bien le distinguer des vraies données
                TaxiRide result = new TaxiRide(
                        medallionInState,
                        "TOTAL_RESULT",
                        "SUM",
                        "END_WINDOW",
                        "END_WINDOW",
                        totalDistance
                );

                System.out.println("GROUPBY: Résultat émis pour " + medallionInState + " = " + totalDistance + "km");
                out.collect(result);
            }
        }
    }

    /**
     * PROP : Propagation
     * On prévient les opérateurs suivants (ex: un Sort) que c'est fini aussi pour eux.
     */
    @Override
    public void prop(Punctuation p, Context context, Collector<StreamItem> out) throws Exception {
        out.collect(p);
    }

    /**
     * KEEP : Nettoyage (Garbage Collection)
     * Une fois le résultat émis, on doit vider la mémoire pour repartir à 0
     * pour la prochaine tranche de 6h.
     */
    @Override
    public void keep(Punctuation p, Context context) throws Exception {
        Iterator<Map.Entry<String, Double>> iterator = runningSums.iterator();

        while (iterator.hasNext()) {
            Map.Entry<String, Double> entry = iterator.next();
            String pattern = p.getMedallion();

            // Si la ponctuation matche, on supprime l'entrée de la mémoire
            if (pattern == null || pattern.equals(entry.getKey())) {
                iterator.remove(); // C'est ici qu'on libère la RAM !
            }
        }
    }
}