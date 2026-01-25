package org.example.framework;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.example.model.StreamItem;
import org.example.model.TaxiRide;

public class PunctuationGenerator implements FlatMapFunction<TaxiRide, StreamItem> {
    @Override
    public void flatMap(TaxiRide taxiRide, Collector<StreamItem> collector) throws Exception {

    }
}
