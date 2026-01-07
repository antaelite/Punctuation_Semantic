package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.model.HourlyPunctuation;
import org.example.model.SensorReading;
import org.example.model.StreamElement;
import org.example.operators.NaiveUnionOperator;
import org.example.operators.PunctuatedUnionOperator;
import org.example.source.TemperatureGeneratorFunction;

/**
 * Main job replicating Tucker et al. 2003 "Exploiting Punctuation Semantics in Continuous Data Streams"
 * Demonstrates:
 * 1. Temperature warehouse scenario (Section 7.2.1)
 * 2. Union operator with duplicate elimination
 * 3. Comparison between naive (unbounded state) and punctuated (bounded state) approaches
 * 4. Replication of Figure 2(a): State Size for Union Operator
 * Expected behavior:
 * - NAIVE: Linear growth of state (memory leak)
 * - PUNCTUATED: Sawtooth pattern with state purged at the end of each hour
 */
public class Main {
    public static void main(String[] args) throws Exception {
        // Configuration matching Tucker et al. 2003 Section 7.3
        final String[] SENSOR_IDS = {"S1", "S2", "S3"}; // Multiple sensors
        final int DURATION_HOURS = 5;  // Simulate 5 hours (paper uses 60, but we use 5 for demo)
        final int READINGS_PER_MINUTE = 2; // Reading frequency

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
        // Create DataGeneratorSource for Stream 1
        TemperatureGeneratorFunction generator1 = new TemperatureGeneratorFunction(SENSOR_IDS, DURATION_HOURS, READINGS_PER_MINUTE);
        DataGeneratorSource<StreamElement> source1 = new DataGeneratorSource<StreamElement>(
                generator1,
                1000L,
                TypeInformation.of(StreamElement.class)
        );

        DataStream<StreamElement> stream1 = env.fromSource(
                source1,
                WatermarkStrategy.noWatermarks(),
                "Sensor Source 1 (Naive)"
        );

        // Create DataGeneratorSource for Stream 2
        // Note: We need a fresh generator instance if it holds state or random seeds, though here it's stateless-ish.
        TemperatureGeneratorFunction generator2 = new TemperatureGeneratorFunction(SENSOR_IDS, DURATION_HOURS, READINGS_PER_MINUTE);
        DataGeneratorSource<StreamElement> source2 = new DataGeneratorSource<StreamElement>(
                generator2,
                1000L,
                TypeInformation.of(StreamElement.class)
        );

        DataStream<StreamElement> stream2 = env.fromSource(
                source2,
                WatermarkStrategy.noWatermarks(),
                "Sensor Source 2 (Punctuated)"
        );

        // Branch 1: Naive Union Operator (WITHOUT punctuation optimization)
        System.out.println("Starting NAIVE Union Operator (unbounded state)...");
        DataStream<String> naiveOutput = stream1
                .keyBy(element -> element.isPunctuation() ?
                        ((HourlyPunctuation) element).getSid() :
                        ((SensorReading) element).getSid())
                .process(new NaiveUnionOperator())
                .name("Naive Union Output");

        // Add sink to naive output
        naiveOutput.print().name("Naive Print Sink");

        // Branch 2: Punctuated Union Operator (WITH punctuation optimization)
        System.out.println("Starting PUNCTUATED Union Operator (bounded state with cleanup)...");
        DataStream<String> punctuatedOutput = stream2
                .keyBy(element -> element.isPunctuation() ?
                        ((HourlyPunctuation) element).getSid() :
                        ((SensorReading) element).getSid())
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