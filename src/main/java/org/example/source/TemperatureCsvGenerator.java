package org.example.source;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Random;

/**
 * Generates temperature data and writes it to a CSV file. CSV Format:
 * type,sid,hour,minute,temperature,timestamp,punctuation_field,punctuation_value
 */
public class TemperatureCsvGenerator {

    public static void main(String[] args) {
        String[] sensorIds = {"S1", "S2", "S3"};
        int durationHours = 5;
        int readingsPerMinute = 2;
        String outputPath = "src/main/resources/temperature.csv";

        try {
            generateCsv(sensorIds, durationHours, readingsPerMinute, outputPath);
            System.out.println("CSV generation complete: " + outputPath);
        } catch (IOException e) {
            System.err.println("Error writing CSV: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void generateCsv(String[] sensorIds, int durationHours, int readingsPerMinute, String outputPath) throws IOException {
        Random random = new Random();

        try (PrintWriter writer = new PrintWriter(new FileWriter(outputPath))) {
            // Write header
            writer.println("type,sid,hour,minute,temperature,timestamp,punctuation_field,punctuation_value");

            for (int hour = 0; hour < durationHours; hour++) {
                // Generate readings for the hour
                for (int minute = 0; minute < 60; minute++) {
                    for (String sensorId : sensorIds) {
                        for (int i = 0; i < readingsPerMinute; i++) {
                            double temperature = 15.0 + random.nextDouble() * 20.0;
                            long timestamp = System.currentTimeMillis(); // Using current time for simplicity, could be logical

                            // type,sid,hour,minute,temperature,timestamp,punctuation_field,punctuation_value
                            writer.printf("READING,%s,%d,%d,%.2f,%d,,%n",
                                    sensorId, hour, minute, temperature, timestamp);
                        }
                    }
                }

                // Generate punctuation at the end of the hour for each sensor
                for (String sensorId : sensorIds) {
                    long timestamp = System.currentTimeMillis();
                    // type,sid,hour,minute,temperature,timestamp,punctuation_field,punctuation_value
                    writer.printf("PUNCTUATION,%s,%d,0,0.0,%d,hour,%d%n",
                            sensorId, hour, timestamp, hour);
                }
            }
        }
    }
}
