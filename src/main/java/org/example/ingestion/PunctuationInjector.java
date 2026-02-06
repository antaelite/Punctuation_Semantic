package org.example.ingestion;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.example.core.StreamItem;
import org.example.model.Punctuation;
import org.example.model.TaxiRide;

public class PunctuationInjector implements FlatMapFunction<TaxiRide, StreamItem> {

    private transient Long lastWindowStart = null;

    // On retire la constante "static final" qui bloquait la logique
    // private static final long WINDOW_SIZE = 21600000L; <--- À SUPPRIMER

    private final long windowSizeMs;
    private final boolean punctuationEnabled;

    public PunctuationInjector(int punctuationsPerDay) {
        if (punctuationsPerDay <= 0) {
            this.punctuationEnabled = false;
            this.windowSizeMs = Long.MAX_VALUE; // Pas de ponctuation (infini)
        } else {
            this.punctuationEnabled = true;
            // 24h * 60min * 60sec * 1000ms = 86 400 000 ms par jour
            this.windowSizeMs = 86400000L / punctuationsPerDay;
        }
    }

    @Override
    public void flatMap(TaxiRide ride, Collector<StreamItem> out) throws Exception {
        if (ride == null) return;

        // 1. Si les ponctuations sont désactivées (Test Baseline), on ne fait rien de spécial
        if (!punctuationEnabled) {
            out.collect(ride);
            return;
        }

        long eventTime = ride.getDropoffTimestamp();

        // 2. CORRECTION CRUCIALE : On utilise windowSizeMs (dynamique) et non WINDOW_SIZE
        long currentWindowStart = (eventTime / windowSizeMs) * windowSizeMs;

        // Initialisation au premier passage
        if (lastWindowStart == null) {
            lastWindowStart = currentWindowStart;
        }

        // Détection de changement de fenêtre
        if (currentWindowStart > lastWindowStart) {
            // La ponctuation ferme la fenêtre PRÉCÉDENTE
            // Elle dit : "Tout ce qui est entre lastWindowStart et (currentWindowStart - 1) est fini"
            long endOfPeriod = currentWindowStart - 1;

            out.collect(new Punctuation(lastWindowStart, endOfPeriod));

            // On met à jour pour la prochaine itération
            lastWindowStart = currentWindowStart;
        }

        out.collect(ride);
    }
}