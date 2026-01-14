package org.example.source;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.example.model.Punctuation;
import org.example.model.SensorReading;
import org.example.model.StreamElement;

/**
 * Reads temperature data from a CSV file in the classpath resources.
 */
public class TemperatureCsvSource implements SourceFunction<StreamElement> {

    private final String csvFilePath;
    private final long speedFactor; // Delay in ms between events to simulate stream speed
    private volatile boolean isRunning = true;

    public TemperatureCsvSource(String csvFilePath) {
        this(csvFilePath, 0);
    }

    public TemperatureCsvSource(String csvFilePath, long speedFactor) {
        this.csvFilePath = csvFilePath;
        this.speedFactor = speedFactor;
    }

    @Override
    public void run(SourceContext<StreamElement> ctx) throws Exception {
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(csvFilePath);
        if (inputStream == null) {
            throw new RuntimeException("CSV file not found in resources: " + csvFilePath);
        }

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String line = reader.readLine(); // Skip header

            while (isRunning && (line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    continue;
                }

                StreamElement element = parseLine(line);
                if (element != null) {
                    ctx.collect(element);

                    if (speedFactor > 0) {
                        Thread.sleep(speedFactor);
                    }
                }
            }
        }
    }

    private StreamElement parseLine(String line) {
        // Format: type,sid,hour,minute,temperature,timestamp,punctuation_field,punctuation_value
        String[] tokens = line.split(",");
        if (tokens.length < 6) {
            return null;
        }

        String type = tokens[0].trim();
        String sid = tokens[1].trim();
        int hour = Integer.parseInt(tokens[2].trim());
        long timestamp = Long.parseLong(tokens[5].trim());

        if ("READING".equalsIgnoreCase(type)) {
            int minute = Integer.parseInt(tokens[3].trim());
            double temperature = Double.parseDouble(tokens[4].trim());
            return new SensorReading(sid, hour, minute, temperature, timestamp);
        } else if ("PUNCTUATION".equalsIgnoreCase(type)) {
            String punctField = tokens[6].trim();
            // Assuming simplified punctuation value as integer for "hour" based on generator
            int punctValue = Integer.parseInt(tokens[7].trim());
            return new Punctuation(sid, punctField, punctValue, timestamp);
        }

        return null;
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
