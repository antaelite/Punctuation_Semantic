package org.example.operators;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.example.model.StreamElement;
import org.example.model.TaxiRide;

/**
 * Mapper that parses CSV lines into TaxiRide objects. The try-catch handles
 * headers and malformed lines gracefully by ignoring them. Note: Punctuation is
 * NOT read from CSV - it's injected by operators if needed.
 */
public class TaxiCsvMapper implements FlatMapFunction<String, StreamElement> {

    @Override
    public void flatMap(String line, Collector<StreamElement> out) {
        try {
            // Parse as TaxiRide
            TaxiRide currentRide = new TaxiRide(line);
            out.collect(currentRide);

        } catch (Exception e) {
            // Ignore headers or malformed lines
        }
    }
}
