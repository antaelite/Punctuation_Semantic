package org.example.operators;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.example.core.PunctuatedIterator;
import org.example.core.StreamItem;
import org.example.model.Punctuation;
import org.example.model.TaxiRide;
import org.example.utils.GeoUtils;

public class StreamCountSameBorough extends PunctuatedIterator {

    // Map: Quartier -> Nombre de trajets
    private transient MapState<String, Integer> count;

    @Override
    public void open(Configuration parameters) {
        GeoUtils.loadBoroughs("src/main/resources/nyc-boroughs.geojson");
        count = getRuntimeContext().getMapState(new MapStateDescriptor<>("q3Counts", String.class, Integer.class));
        System.out.println("Count the number of rides that start and end in the same borough.");
    }

    @Override
    public void step(TaxiRide ride, Context context, Collector<StreamItem> out) throws Exception {
        String start = GeoUtils.getBorough(Double.parseDouble(ride.pickupLongitude), Double.parseDouble(ride.pickupLatitude));
        String end = GeoUtils.getBorough(Double.parseDouble(ride.dropoffLongitude), Double.parseDouble(ride.dropoffLatitude));

        // LOGIQUE Q3 : Départ == Arrivée
        if (!"Unknown".equals(start) && start.equals(end)) {
            Integer c = count.get(start);
            count.put(start, (c == null ? 0 : c) + 1);
        }
    }

    @Override public void pass(Punctuation p, Context ctx, Collector<StreamItem> out) {}
    @Override public void prop(Punctuation p, Context ctx, Collector<StreamItem> out) { out.collect(p); }

    @Override
    public void keep(Punctuation p, Context context) throws Exception {
        for (String b : count.keys()) {
            System.out.println("  " + b + " : " + count.get(b));
        }
//        count.clear(); // Nettoyage (Sawtooth)
    }
}