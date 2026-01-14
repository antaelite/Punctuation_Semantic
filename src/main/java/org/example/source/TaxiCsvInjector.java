package org.example.source;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Utility to read the sorted Taxi CSV and inject explicit PUNCTUATION rows at
 * hour boundaries.
 */
public class TaxiCsvInjector {

    public static void main(String[] args) {
        String inputPath = "src/main/resources/nyc_taxi_sorted.csv";
        String outputPath = "src/main/resources/nyc_taxi_punctuated.csv";

        System.out.println("Reading from: " + inputPath);
        System.out.println("Writing to: " + outputPath);

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        try (BufferedReader reader = new BufferedReader(new FileReader(inputPath)); PrintWriter writer = new PrintWriter(new FileWriter(outputPath))) {

            String line;
            int currentHour = -1;

            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    continue;
                }

                try {
                    // CSV Format: medallion, hack_license, vendor_id, rate_code, store_and_fwd_flag, pickup_datetime, ...
                    String[] tokens = line.split(",");
                    // pickup_datetime is at index 5
                    if (tokens.length < 6) {
                        continue;
                    }
                    String pickupTimeStr = tokens[5];

                    // Header check
                    if (pickupTimeStr.equalsIgnoreCase("pickup_datetime")) {
                        writer.println(line);
                        continue;
                    }

                    LocalDateTime pickupTime = LocalDateTime.parse(pickupTimeStr, formatter);
                    int rowHour = pickupTime.getHour();

                    if (currentHour == -1) {
                        currentHour = rowHour;
                    }

                    if (rowHour > currentHour) {
                        // 1. EMIT PUNCTUATION
                        // Using 'medallion' column for PUNCTUATION tag
                        // Boundary time: same date at rowHour:00:00
                        LocalDateTime boundaryTime = pickupTime.withMinute(0).withSecond(0).withNano(0);

                        // medallion (index 0) = PUNCTUATION
                        // pickup_datetime (index 5) = boundaryTime
                        // Other columns empty
                        StringBuilder sb = new StringBuilder();
                        sb.append("PUNCTUATION,"); // 0
                        sb.append(",,,");          // 1-3
                        sb.append(",");            // 4
                        sb.append(boundaryTime.format(formatter)).append(","); // 5
                        sb.append(",,,,,,,,");     // Rest (assuming ~14 cols)

                        writer.println(sb.toString());
                        System.out.println("Injected punctuation for boundary hour: " + rowHour);

                        currentHour = rowHour;
                    }

                    // 3. Always emit actual row
                    writer.println(line);

                } catch (Exception e) {
                    // Ignore parsing errors
                }
            }

            System.out.println("Done.");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
