package org.example.source;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.example.model.Punctuation;
import org.example.model.StreamElement;
import org.example.model.TaxiRide;

/**
 * Mapper that parses CSV lines into TaxiRide objects and injects specific
 * punctuation. Replaces the logic previously held in TaxiFileSource.
 */
public class TaxiPunctuationMapper implements FlatMapFunction<String, StreamElement> {

    @Override
    public void flatMap(String line, Collector<StreamElement> out) {
        try {
            // Check for Punctuation in first column (medallion)
            String[] tokens = line.split(",");
            if (tokens.length > 0 && "PUNCTUATION".equals(tokens[0])) {
                // We expect timestamp in column 5 (pickup_datetime)
                if (tokens.length > 5) {
                    String timeStr = tokens[5];
                    // Parse the time to get the hour and timestamp
                    java.time.format.DateTimeFormatter formatter = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                    java.time.LocalDateTime dt = java.time.LocalDateTime.parse(timeStr, formatter);

                    long timestamp = java.sql.Timestamp.valueOf(dt).getTime();
                    int hour = dt.getHour();

                    System.out.println(">>> SOURCE: Read PUNCTUATION for hour " + hour);
                    out.collect(new Punctuation("NYC-TAXI", "hour", hour, timestamp));
                }
                return;
            }

            // Otherwise parse as TaxiRide
            TaxiRide currentRide = new TaxiRide(line);
            out.collect(currentRide);

        } catch (Exception e) {
            // Ignore headers or malformed lines
        }
    }
}
