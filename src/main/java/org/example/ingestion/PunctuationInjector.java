package org.example.ingestion;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.example.core.StreamItem;
import org.example.model.Punctuation;
import org.example.model.TaxiRide;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Instant;

public class PunctuationInjector implements FlatMapFunction<TaxiRide, StreamItem> {

    private transient String lastMedallion = null; // Maybe use Flink State to store these values
    private transient Long lastWindowStart = null;
    private static final long WINDOW_SIZE = 21600000L; // 6 heures en ms
    private static final Logger LOG = LoggerFactory.getLogger(PunctuationInjector.class);

    @Override
    public void flatMap(TaxiRide ride, Collector<StreamItem> out) throws Exception {
        if (ride == null) {
            return;
        }
        long currentWindowStart = (ride.getPickupTimestamp() / WINDOW_SIZE) * WINDOW_SIZE;

        // 1. CHANGEMENT DE TAXI
        if (lastMedallion != null && !lastMedallion.equals(ride.medallion)) {
//            LOG.warn("Taxi change: {} -> {}", lastMedallion, ride.medallion);
            out.collect(new Punctuation(lastMedallion));

            lastWindowStart = null;  // Reset for new taxi
        } // 2. CHANGEMENT DE FENÊTRE (00h->06h, etc.)
        else if (lastWindowStart != null && currentWindowStart > lastWindowStart) {
            long endOfPeriod = currentWindowStart - 1;

//            LOG.warn("Window change for {}: [{} -> {}]", ride.medallion, lastWindowStart, currentWindowStart);
            out.collect(new Punctuation(ride.medallion, lastWindowStart, endOfPeriod));
        }

        // 3. Émission Donnée
        out.collect(ride);

        lastMedallion = ride.medallion;
        lastWindowStart = currentWindowStart;
    }
}
