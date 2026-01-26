package org.example.ingestion;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.example.core.StreamItem;
import org.example.model.Punctuation;
import org.example.model.TaxiRide;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PunctuationInjector implements FlatMapFunction<TaxiRide, StreamItem> {

    private transient Long lastWindowStart = null;
    private static final long WINDOW_SIZE = 21600000L; // 6 heures en ms
    private static final Logger LOG = LoggerFactory.getLogger(PunctuationInjector.class);

    @Override
    public void flatMap(TaxiRide ride, Collector<StreamItem> out) throws Exception {
        if (ride == null) {
            return;
        }

        // --- CHANGEMENT ICI : On utilise Dropoff (Arrivée) ---
        long eventTime = ride.getDropoffTimestamp();

        // Calcul de la fenêtre basé sur l'arrivée
        long currentWindowStart = (eventTime / WINDOW_SIZE) * WINDOW_SIZE;

        // Détection du saut de fenêtre
        if (lastWindowStart != null && currentWindowStart > lastWindowStart) {
            long endOfPeriod = currentWindowStart - 1;

            // On émet la ponctuation pour fermer la fenêtre précédente
            out.collect(new Punctuation(lastWindowStart, endOfPeriod));

            // LOG.info("Fin de la fenêtre {} (basée sur drop-off)", lastWindowStart);
        }

        // Émission de la donnée
        out.collect(ride);

        // Mise à jour de l'état
        lastWindowStart = currentWindowStart;
    }
}