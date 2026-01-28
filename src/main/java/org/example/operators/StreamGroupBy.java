package org.example.operators;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import org.example.model.GroupByResult;
import org.example.core.PunctuatedIterator;
import org.example.core.StreamItem;
import org.example.model.Punctuation;
import org.example.model.TaxiRide;

import java.time.Instant;
import java.util.Map;

public class StreamGroupBy extends PunctuatedIterator {

    // Mémoire : Associe un Médaillon (String) à une Distance cumulée (Double)
    private MapState<String, Double> runningSums;
    private ValueState<Long> lastClosedWindowEnd;

    @Override
    public void open(Configuration parameters) {
        runningSums = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("runningSums", String.class, Double.class)
        );
        lastClosedWindowEnd = getRuntimeContext().getState(
                new ValueStateDescriptor<>("lastClosedWindow", Long.class)
        );
    }


    @Override
    public void step(TaxiRide ride, Context context, Collector<StreamItem> out) throws Exception {
        Long threshold = lastClosedWindowEnd.value();

        if (threshold != null && ride.getDropoffTimestamp() <= threshold) {
            System.out.println("LATE DATA DROPPED: " + ride.medallion + " at " + ride.dropoffDatetime);
            return;
        }
        Double currentTotal = runningSums.get(ride.medallion);
        if (currentTotal == null) {
            currentTotal = 0.0;
        }
        runningSums.put(ride.medallion, currentTotal + ride.tripDistance);
    }

    @Override
    public void pass(Punctuation p, Context context, Collector<StreamItem> out) throws Exception {

        for (Map.Entry<String, Double> entry : runningSums.entries()) {
            String medallion = entry.getKey();
            Double totalDistance = entry.getValue();

            String windowStart = Instant.ofEpochMilli(p.getStart()).toString();
            String windowEnd = Instant.ofEpochMilli(p.getEnd()).toString();

            out.collect(new GroupByResult(medallion, totalDistance, windowStart, windowEnd));

        }
    }

    @Override
    public void keep(Punctuation p, Context context) throws Exception {

        runningSums.clear();
        lastClosedWindowEnd.update(p.getEnd());
    }


    @Override
    public void prop(Punctuation p, Context context, Collector<StreamItem> out) {
        out.collect(p);
    }
}
