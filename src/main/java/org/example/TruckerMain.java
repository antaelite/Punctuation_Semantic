package org.example;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.model.HourlyPunctuation;
import org.example.model.StreamElement;
import org.example.model.TaxiRide;
import org.example.operators.NaiveUnionTaxi;
import org.example.operators.PunctuatedUnionTaxi;
import org.example.source.TaxiFileSource;

/**
 * Main job replicating Tucker et al. 2003 with NYC Taxi Dataset.
 * * Scenario: Query 3 (Trips per hour analysis)
 * - Source: Real-world CSV Data (NYC Taxi)
 * - Punctuation: Injected by Source based on CSV timestamps
 * - Comparison: Naive (Unbounded State) vs Punctuated (Bounded State)
 */
public class TruckerMain {
    public static void main(String[] args) throws Exception {

        // --- CONFIGURATION ---
        // Pensez à bien changer le chemin absolu de la source dans ton ordi
        final String CSV_FILE_PATH = "C:\\Users\\antag\\Downloads\\sample.csv";

        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //  1 seul thread pour respecter l'ordre chronologique du fichier CSV

        System.out.println("=================================================");
        System.out.println("Tucker et al. 2003 - NYC Taxi Replication");
        System.out.println("=================================================");
        System.out.println("Dataset: " + CSV_FILE_PATH);
        System.out.println("Objective: Demonstrate State Cleanup using Punctuation on Real Data");
        System.out.println("=================================================\n");

        // --- SOURCE 1: NAIVE STREAM ---
        // On crée une première lecture du fichier pour l'opérateur naïf
        DataStream<StreamElement> stream1 = env.addSource(new TaxiFileSource(CSV_FILE_PATH))
                .name("Taxi Source (Naive)");

        // --- SOURCE 2: PUNCTUATED STREAM ---
        // On crée une deuxième lecture indépendante pour l'opérateur optimisé
        DataStream<StreamElement> stream2 = env.addSource(new TaxiFileSource(CSV_FILE_PATH))
                .name("Taxi Source (Punctuated)");

        // --- BRANCH 1: NAIVE OPERATOR (Linear Growth) ---
        System.out.println("Configuring NAIVE Operator pipeline...");
        DataStream<String> naiveOutput = stream1
                // On groupe tout sous une même clé pour voir l'accumulation globale de la mémoire
                .keyBy(element -> {
                    if (element.isPunctuation()) {
                        return ((HourlyPunctuation) element).getKeyTaxi(); // Retourne "NYC-TAXI"
                    } else {
                        return ((TaxiRide) element).getKeyTaxi();          // Retourne "NYC-TAXI"
                    }
                })
                .process(new NaiveUnionTaxi())
                .name("Naive Operator (Unbounded)");

        naiveOutput.print().name("Naive-Sink");

        // --- BRANCH 2: PUNCTUATED OPERATOR (Sawtooth Pattern) ---
        System.out.println("Configuring PUNCTUATED Operator pipeline...");
        DataStream<String> punctuatedOutput = stream2
                // Même clé de regroupement
                .keyBy(element -> {
                    if (element.isPunctuation()) {
                        return ((HourlyPunctuation) element).getKeyTaxi();
                    } else {
                        return ((TaxiRide) element).getKeyTaxi();
                    }
                })
                .process(new PunctuatedUnionTaxi())
                .name("Punctuated Operator (Bounded)");

        punctuatedOutput.print().name("Punctuated-Sink");

        // --- EXECUTION ---
        System.out.println("Starting Job...");
        env.execute("Tucker Replication - NYC Taxi");
    }
}
