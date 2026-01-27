package org.example.operators;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;

import org.apache.flink.util.Collector;
import org.example.core.PunctuatedIterator;
import org.example.model.Punctuation;
import org.example.core.StreamItem;
import org.example.model.TaxiRide;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class StreamDuplicateElimination extends PunctuatedIterator {

    private MapState<String, TaxiRide> seenTuples;
    private ValueState<Long> lastClosedWindowEnd;

    @Override
    public void open(Configuration parameters) {
        MapStateDescriptor<String, TaxiRide> descriptor = new MapStateDescriptor<>("seenTuples", String.class, TaxiRide.class);
        seenTuples = getRuntimeContext().getMapState(descriptor);
        lastClosedWindowEnd = getRuntimeContext().getState(
                new ValueStateDescriptor<>("lastClosedWindow", Long.class)
        );
    }

    @Override
    public void step(TaxiRide taxiRide, Context context, Collector<StreamItem> out) throws Exception {
        Long threshold = lastClosedWindowEnd.value();

        if (threshold != null && taxiRide.getDropoffTimestamp() <= threshold) {
            System.out.println("LATE DATA DROPPED: " + taxiRide.medallion + " at " + taxiRide.dropoffDatetime);
            return;
        }
        // Use the hashCode of the entire TaxiRide object as the key for exact duplicate detection
        String key = taxiRide.medallion + taxiRide.pickupDatetime;
        if (!seenTuples.contains(key)) {
            seenTuples.put(key, taxiRide);
            out.collect(taxiRide);
        } else {
            System.out.println("Dublicate: " +  taxiRide.medallion );
        }
    }

    @Override
    public void pass(Punctuation p, Context context, Collector<StreamItem> out) {
        // sdupelim is not a blocking operator, so pass is trivial (returns empty)
    }

    @Override
    public void prop(Punctuation p, Context context, Collector<StreamItem> out) {
        // Simply propagates the punctuation as it arrives.
        // startTime =  min de startime actuel et de ponctuation actuel
        // endtime =  max de endtime et de la punctation
        out.collect(p);
    }

    @Override
    public void keep(Punctuation p, Context context) throws Exception {
        // Collect keys to remove
        List<String> keysToRemove = new ArrayList<>();

        for (Map.Entry<String, TaxiRide> entry : seenTuples.entries()) {
            if (p.match(entry.getValue())) {
                keysToRemove.add(entry.getKey());
            }
        }

        // Remove them
        for (String key : keysToRemove) {
            System.out.println("Removed: " + seenTuples.get(key).medallion);
            seenTuples.remove(key); // TODO: load into a database
//            System.out.println("Removed: " + key);

        }
        lastClosedWindowEnd.update(p.getEndTimestamp());
    }
}
