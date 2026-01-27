package org.example.operators;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.example.core.PunctuatedIterator;
import org.example.core.StreamItem;
import org.example.model.BoroughCountItem;
import org.example.model.Punctuation;
import org.example.model.TaxiRide;
import org.example.utils.GeoUtils;

import java.io.IOException;

public class StreamCountDiffBorough extends PunctuatedIterator {

    private transient ValueState<Integer> countTrips;

    @Override
    public void open(Configuration parameters) {
        GeoUtils.loadBoroughs("src/main/resources/nyc-boroughs.geojson");
        countTrips = getRuntimeContext().getState(new ValueStateDescriptor<>("CountDiff", Integer.class));
        System.out.println("Count the number of rides that start and end in different borough.");
    }

    @Override
    public void step(TaxiRide ride, Context context, Collector<StreamItem> out) throws Exception {
        String start = GeoUtils.getBorough(Double.parseDouble(ride.pickupLongitude), Double.parseDouble(ride.pickupLatitude));
        String end = GeoUtils.getBorough(Double.parseDouble(ride.dropoffLongitude), Double.parseDouble(ride.dropoffLatitude));

        // Départ != Arrivée
        if (!"Unknown".equals(start) && !start.equals(end)) {
            countTrips.update((countTrips.value() == null ? 0 : countTrips.value()) + 1);
        }
    }

    @Override public void pass(Punctuation p, Context ctx, Collector<StreamItem> out) throws IOException {
        out.collect(new BoroughCountItem("All", countTrips.value() == null ? 0 : countTrips.value()));
    }
    @Override public void prop(Punctuation p, Context ctx, Collector<StreamItem> out) { out.collect(p); }

    @Override
    public void keep(Punctuation p, Context context)  {
        countTrips.clear(); // Clear state
    }
}