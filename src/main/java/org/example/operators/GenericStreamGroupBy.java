package org.example.operators;

import org.apache.flink.api.common.state.MapState;
import org.example.core.*;
import org.example.model.Punctuation;
import org.example.model.TaxiRide;


import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.Map;
import java.util.function.Function;

public class GenericStreamGroupBy extends PunctuatedIterator {

    private final SerializableKeyExtractor keyExtractor;
    private final AggregationStrategy strategy;
    private MapState<String, Double> state;

    public GenericStreamGroupBy(SerializableKeyExtractor keyExtractor, AggregationStrategy strategy) {
        this.keyExtractor = keyExtractor;
        this.strategy = strategy;
    }         // L'opération (SUM, MAX, etc.)




    @Override
    public void open(Configuration parameters) {
        state = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("genericState", String.class, Double.class)
        );
    }

    @Override
    public void step(TaxiRide ride, Context context, Collector<StreamItem> out) throws Exception {
        String key = keyExtractor.apply(ride);
        Double current = state.get(key);
        if (current == null) current = 0.0;
        state.put(key, strategy.aggregate(current, ride));
    }

    @Override
    public void pass(Punctuation p, Context context, Collector<StreamItem> out) throws Exception {
        for (Map.Entry<String, Double> entry : state.entries()) {
            String start = Instant.ofEpochMilli(p.getStartTimestamp()).toString();
            String end = Instant.ofEpochMilli(p.getEndTimestamp()).toString();

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
    }

    @Override
    public void prop(Punctuation p, Context context, Collector<StreamItem> out) throws Exception {
        out.collect(p);
    }
}