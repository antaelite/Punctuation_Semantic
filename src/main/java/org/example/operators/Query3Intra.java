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

public class Query3Intra extends PunctuatedIterator {

    // Map: Quartier -> Nombre de trajets
    private transient MapState<String, Integer> counts;

    @Override
    public void open(Configuration parameters) {
        GeoUtils.loadBoroughs("src/main/resources/nyc-boroughs.geojson");

        counts = getRuntimeContext().getMapState(new MapStateDescriptor<>("q3Counts", String.class, Integer.class));
    }

    @Override
    public void step(TaxiRide ride, Context context, Collector<StreamItem> out) throws Exception {
        String start = GeoUtils.getBorough(Double.parseDouble(ride.pickupLongitude), Double.parseDouble(ride.pickupLatitude));
        String end = GeoUtils.getBorough(Double.parseDouble(ride.dropoffLongitude), Double.parseDouble(ride.dropoffLatitude));

        // LOGIQUE Q3 : Départ == Arrivée
        if (!"Unknown".equals(start) && start.equals(end)) {
            Integer c = counts.get(start);
            counts.put(start, (c == null ? 0 : c) + 1);
        }
    }

    @Override public void pass(Punctuation p, Context ctx, Collector<StreamItem> out) {}
    @Override public void prop(Punctuation p, Context ctx, Collector<StreamItem> out) { out.collect(p); }

    @Override
    public void keep(Punctuation p, Context context) throws Exception {
        System.out.println("\n=== RÉSULTATS QUERY 3 (Intra-Borough) ===");
        for (String b : counts.keys()) {
            System.out.println("  " + b + " : " + counts.get(b));
        }
        counts.clear(); // Nettoyage (Sawtooth)
    }
}