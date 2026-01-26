package org.example.ingestion;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import org.example.core.StreamItem;
import org.example.model.Punctuation;
import org.example.model.TaxiRide;

public class PunctuationGenerator implements FlatMapFunction<TaxiRide, StreamItem> {
    private String lastMedallion = null ;
    @Override
    public void flatMap(TaxiRide ride, Collector<StreamItem> out) throws Exception {
        if (ride == null) {
            return;
        }

        String currentMedallion = ride.medallion;
        if (lastMedallion != null && !lastMedallion.equals(currentMedallion)) {

            out.collect(new Punctuation(lastMedallion));


            System.out.println("GEN: Punctuation émise pour " + lastMedallion);
        }


        out.collect(ride);


        lastMedallion = currentMedallion;
    }
}


