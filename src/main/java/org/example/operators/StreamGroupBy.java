package org.example.operators;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import org.example.core.GroupByResult;
import org.example.core.PunctuatedIterator;
import org.example.core.StreamItem;
import org.example.model.Punctuation;
import org.example.model.TaxiRide;

import java.time.Instant;
import java.util.Map;

public class StreamGroupBy extends PunctuatedIterator {

    // Mémoire : Associe un Médaillon (String) à une Distance cumulée (Double)
    private MapState<String, Double> runningSums;

    @Override
    public void open(Configuration parameters) {
        runningSums = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("runningSums", String.class, Double.class)
        );
    }

    /**
     * STEP : Accumulation On additionne les distances au fur et à mesure que
     * les taxis arrivent.
     */
    @Override
    public void step(TaxiRide ride, Context context, Collector<StreamItem> out) throws Exception {
        Double currentTotal = runningSums.get(ride.medallion);
        if (currentTotal == null) {
            currentTotal = 0.0;
        }
        runningSums.put(ride.medallion, currentTotal + ride.tripDistance);
    }

    /**
     * PASS : Émission La ponctuation temporelle arrive. C'est le signal que la
     * tranche de 6h est finie. On émet TOUT ce qu'on a accumulé en mémoire.
     */
    @Override
    public void pass(Punctuation p, Context context, Collector<StreamItem> out) throws Exception {
        // On parcourt tous les taxis en mémoire
        for (Map.Entry<String, Double> entry : runningSums.entries()) {
            String medallion = entry.getKey();
            Double totalDistance = entry.getValue();

            // Astuce : On utilise les dates de la Punctuation pour rendre le résultat lisible
            String windowStart = Instant.ofEpochMilli(p.getStartTimestamp()).toString();
            String windowEnd = Instant.ofEpochMilli(p.getEndTimestamp()).toString();

            out.collect(new GroupByResult(medallion,totalDistance, windowStart, windowEnd));

        }
    }

    /**
     * KEEP : Nettoyage La fenêtre est finie pour tout le monde. On vide la
     * mémoire pour repartir à zéro pour la prochaine tranche de 6h.
     */
    @Override
    public void keep(Punctuation p, Context context) throws Exception {
        // Nettoyage radical : on vide toute la map
        runningSums.clear();
    }

    /**
     * PROP : Propagation On transmet l'ordre de fin aux opérateurs suivants.
     */
    @Override
    public void prop(Punctuation p, Context context, Collector<StreamItem> out) throws Exception {
        out.collect(p);
    }
}
