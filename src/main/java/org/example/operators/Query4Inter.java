package org.example.operators;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.example.core.PunctuatedIterator;
import org.example.core.StreamItem;
import org.example.model.Punctuation;
import org.example.model.TaxiRide;
import org.example.utils.GeoUtils;

public class Query4Inter extends PunctuatedIterator {

    private transient ValueState<Integer> totalInterTrips;

    @Override
    public void open(Configuration parameters) {
        GeoUtils.loadBoroughs("src/main/resources/nyc-boroughs.geojson");
        totalInterTrips = getRuntimeContext().getState(new ValueStateDescriptor<>("q4Count", Integer.class));
    }

    @Override
    public void step(TaxiRide ride, Context context, Collector<StreamItem> out) throws Exception {
        String start = GeoUtils.getBorough(Double.parseDouble(ride.pickupLongitude), Double.parseDouble(ride.pickupLatitude));
        String end = GeoUtils.getBorough(Double.parseDouble(ride.dropoffLongitude), Double.parseDouble(ride.dropoffLatitude));

        // LOGIQUE Q4 : Départ != Arrivée
        if (!"Unknown".equals(start) && !start.equals(end)) {
            Integer c = totalInterTrips.value();
            totalInterTrips.update((c == null ? 0 : c) + 1);
        }
    }

    @Override public void pass(Punctuation p, Context ctx, Collector<StreamItem> out) {}
    @Override public void prop(Punctuation p, Context ctx, Collector<StreamItem> out) { out.collect(p); }

    @Override
    public void keep(Punctuation p, Context context) throws Exception {
        Integer c = totalInterTrips.value();
        System.out.println("\n=== RÉSULTATS QUERY 4 (Inter-Borough) ===");
        System.out.println("  Nombre de trajets traversant des quartiers : " + (c==null?0:c));
        totalInterTrips.clear();
    }
}