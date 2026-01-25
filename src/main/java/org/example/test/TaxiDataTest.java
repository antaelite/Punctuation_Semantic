package org.example.test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.example.model.TaxiRide;
import org.example.source.TaxiDataSource;

public class TaxiDataTest {

    public static void main(String[] args) {
        // Create the data source mapper
        TaxiDataSource mapper = new TaxiDataSource();

        // Path to the CSV file - adjust this path to your actual CSV file location
        String csvFilePath = "src/main/resources/sample.csv";

        System.out.println("Reading taxi data from: " + csvFilePath);
        System.out.println("==========================================\n");

        int count = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(csvFilePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                try {
                    TaxiRide ride = mapper.map(line);
                    if (ride != null) {
                        count++;
                        System.out.println(count + ". " + ride);
                    }
                } catch (Exception e) {
                    System.err.println("Error processing line: " + e.getMessage());
                }
            }
            System.out.println("\n==========================================");
            System.out.println("Total rides read: " + count);
        } catch (IOException e) {
            System.err.println("Error reading file: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
