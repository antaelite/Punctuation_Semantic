package org.example.operators;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.example.core.*;
import org.example.model.*;


import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.Map;

public class GenericStreamGroupBy extends PunctuatedIterator {

    private final SerializableKeyExtractor keyExtractor;
    private final AggregationStrategy strategy;
    private MapState<String, Double> state;
    private ValueState<Long> lastClosedWindowEnd;

    public GenericStreamGroupBy(SerializableKeyExtractor keyExtractor, AggregationStrategy strategy) {
        this.keyExtractor = keyExtractor;
        this.strategy = strategy;
    }         // L'opération (SUM, MAX, etc.)




    @Override
    public void open(Configuration parameters) {
        state = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("genericState", String.class, Double.class)
        );
        lastClosedWindowEnd = getRuntimeContext().getState(
                new ValueStateDescriptor<>("lastClosedWindow", Long.class)
        );
    }

    @Override
    public void step(TaxiRide ride, Context context, Collector<StreamItem> out) throws Exception {
        String key = keyExtractor.apply(ride);
        Double current = state.get(key);
        Long threshold = lastClosedWindowEnd.value();

        if (threshold != null && ride.getDropoffTimestamp() <= threshold) {
            System.out.println("LATE DATA DROPPED: " + ride.medallion + " at " + ride.dropoffDatetime);
            return;
        }
        if (current == null) current = 0.0;

        state.put(key, strategy.aggregate(current, ride));
    }

    @Override
    public void pass(Punctuation p, Context context, Collector<StreamItem> out) throws Exception {
        for (Map.Entry<String, Double> entry : state.entries()) {
            String start = Instant.ofEpochMilli(p.getStart()).toString();
            String end = Instant.ofEpochMilli(p.getEnd()).toString();

            // On utilise le constructeur adapté
            out.collect(new GroupByResult(
                    entry.getKey(),
                    entry.getValue(),
                    start,
                    end
            ));
        }
    }
    @Override
    public void keep(Punctuation p, Context context) throws Exception {
        state.clear();
        lastClosedWindowEnd.update(p.getEnd());
    }

    @Override
    public void prop(Punctuation p, Context context, Collector<StreamItem> out) {
        out.collect(p);
    }
}