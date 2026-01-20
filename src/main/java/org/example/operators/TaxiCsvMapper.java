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

    private int count = 0;

    @Override
    public void flatMap(String line, Collector<StreamElement> out) {
        try {
            // Parse as TaxiRide
            TaxiRide currentRide = new TaxiRide(line);
            out.collect(currentRide);

            count++;
            if (count % 1000 == 0) {
                System.out.println(">>> PARSED: " + count + " rides so far");
            }

        } catch (Exception e) {
            // Ignore headers or malformed lines - but log first few for debugging
            if (count < 3) {
                System.out.println(">>> CSV-PARSER ERROR: " + e.getClass().getSimpleName()
                        + ": " + e.getMessage());
                System.out.println(">>> Failed line: " + line.substring(0, Math.min(100, line.length())));
            }
        }
    }
}
