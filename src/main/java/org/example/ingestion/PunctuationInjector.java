package org.example.ingestion;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.example.core.StreamItem;
import org.example.model.Punctuation;
import org.example.model.TaxiRide;

public class PunctuationInjector implements FlatMapFunction<TaxiRide, StreamItem> {

    private transient Long lastWindowStart = null;
    private static final long WINDOW_SIZE = 21600000L; // 6 heures en ms

    @Override
    public void flatMap(TaxiRide ride, Collector<StreamItem> out) throws Exception {
        if (ride == null) return;

        long eventTime = ride.getDropoffTimestamp();
        long currentWindowStart = (eventTime / WINDOW_SIZE) * WINDOW_SIZE;


        if (lastWindowStart == null) {
            lastWindowStart = currentWindowStart;
        }


        if (currentWindowStart > lastWindowStart) {
            long endOfPeriod = currentWindowStart - 1;


            out.collect(new Punctuation(lastWindowStart, endOfPeriod));

            lastWindowStart = currentWindowStart;
        }

        out.collect(ride);
    }
}