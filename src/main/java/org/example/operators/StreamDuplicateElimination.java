package org.example.operators;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;

import org.apache.flink.util.Collector;
import org.example.core.PunctuatedIterator;
import org.example.model.Punctuation;
import org.example.core.StreamItem;
import org.example.model.TaxiRide;

import java.util.Iterator;
import java.util.Map;

public class StreamDuplicateElimination extends PunctuatedIterator {

    private MapState<String, TaxiRide> seenTuples;

    @Override
    public void open(Configuration parameters) {
        MapStateDescriptor<String, TaxiRide> descriptor = new MapStateDescriptor<>("seenTuples", String.class, TaxiRide.class);
        seenTuples = getRuntimeContext().getMapState(descriptor);
    }

    @Override
    public void step(TaxiRide taxiRide, Context context, Collector<StreamItem> out) throws Exception {
        // Use the hashCode of the entire TaxiRide object as the key for exact duplicate detection
        String key = String.valueOf(taxiRide.hashCode());
        if (!seenTuples.contains(key)) {
            seenTuples.put(key, taxiRide);
            out.collect(taxiRide);
        }
    }

    @Override
    public void pass(Punctuation p, Context context, Collector<StreamItem> out) {
        // sdupelim is not a blocking operator, so pass is trivial (returns empty)
    }

    @Override
    public void prop(Punctuation p, Context context, Collector<StreamItem> out) {
        // Simply propagates the punctuation as it arrives.
            out.collect(p);
    }

    @Override
    public void keep(Punctuation p, Context context) throws Exception {
        // Any tuples in state that we know can have no more duplicate values can be removed
        Iterator<Map.Entry<String, TaxiRide>> iterator = seenTuples.iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, TaxiRide> entry = iterator.next();
            TaxiRide ride = entry.getValue();
            if (p.match(ride)) {
                iterator.remove();
            }
        }
    }
}
