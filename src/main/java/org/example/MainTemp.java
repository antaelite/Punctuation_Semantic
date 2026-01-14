package org.example;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.model.Punctuation;
import org.example.model.SensorReading;
import org.example.model.StreamElement;
import org.example.operators.NaiveUnionOperator;
import org.example.operators.PunctuatedUnionOperator;
import org.example.source.TemperatureCsvSource;

/**
 * Main job replicating Tucker et al. 2003 "Exploiting Punctuation Semantics in
 * Continuous Data Streams". Compares naive (unbounded state) and punctuated
 * (bounded state) approaches. Expected behavior: - NAIVE: Linear growth of
 * state (memory leak) - PUNCTUATED: Sawtooth pattern with state purged at the
 * end of each hour
 */
public class MainTemp {

    public static void main(String[] args) throws Exception {
        // Configuration matching Tucker et al. 2003 Section 7.3
        final String[] SENSOR_IDS = {"S1", "S2", "S3"}; // Multiple sensors
        final int DURATION_HOURS = 5; // Simulate 5 hours

        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // Single parallelism to ensure ordered processing

        System.out.println("=================================================");
        System.out.println("Tucker et al. 2003 Punctuation Semantics Demo");
        System.out.println("=================================================");
        System.out.println("Scenario: Temperature Warehouse (Section 7.2.1)");
        System.out.println("Sensors: " + String.join(", ", SENSOR_IDS));
        System.out.println("Duration: " + DURATION_HOURS + " hours");
        System.out.println("=================================================\n");

        // Create TWO separate sources (one for each operator)
        // Create Csv Source for Stream 1
        String csvPath = "temperature.csv";
        TemperatureCsvSource source1 = new TemperatureCsvSource(csvPath);

        DataStream<StreamElement> stream1 = env.addSource(source1)
                .name("Sensor Source 1 (Naive)");

        // Create Csv Source for Stream 2
        // We can reuse the file but need a new instance/reader
        TemperatureCsvSource source2 = new TemperatureCsvSource(csvPath);

        DataStream<StreamElement> stream2 = env.addSource(source2)
                .name("Sensor Source 2 (Punctuated)");

        // Branch 1: Naive Union Operator (WITHOUT punctuation optimization)
        System.out.println("Starting NAIVE Union Operator (unbounded state)...");
        DataStream<String> naiveOutput = stream1
                .keyBy(element -> element.isPunctuation() ? ((Punctuation) element).getKey()
                : ((SensorReading) element).getSid())
                .process(new NaiveUnionOperator())
                .name("Naive Union Output");

        // Add sink to naive output
        naiveOutput.print().name("Naive Print Sink");

        // Branch 2: Punctuated Union Operator (WITH punctuation optimization)
        System.out.println("Starting PUNCTUATED Union Operator (bounded state with cleanup)...");
        DataStream<String> punctuatedOutput = stream2
                .keyBy(element -> element.isPunctuation() ? ((Punctuation) element).getKey()
                : ((SensorReading) element).getSid())
                .process(new PunctuatedUnionOperator())
                .name("Punctuated Union Output");

        // Add sink to punctuated output
        punctuatedOutput.print().name("Punctuated Print Sink");

        // Execute the job
        env.execute("Tucker et al. 2003 - Punctuation Semantics Replication");

        System.out.println("\n=================================================");
        System.out.println("Job completed!");
        System.out.println("Expected observations:");
        System.out.println("- NAIVE: State grows linearly (never cleaned up)");
        System.out.println("- PUNCTUATED: State shows sawtooth pattern (cleaned at hour boundaries)");
        System.out.println("=================================================");
    }
}
