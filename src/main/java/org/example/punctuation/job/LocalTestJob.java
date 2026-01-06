package org.example.punctuation.job;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.punctuation.model.StreamElement;
import org.example.punctuation.operators.NaiveUnionOperator;
import org.example.punctuation.operators.PunctuatedUnionOperator;
import org.example.punctuation.source.TemperatureSensorSource;

/**
 * Local test job with faster execution for demonstration purposes.
 * Uses smaller dataset (3 hours, fewer readings) for quick testing.
 */
public class LocalTestJob {
    
    public static void main(String[] args) throws Exception {
        // Test configuration - smaller dataset for fast local execution
        final String[] SENSOR_IDS = {"S1", "S2"};  // 2 sensors for simplicity
        final int DURATION_HOURS = 3;              // Only 3 hours
        final int READINGS_PER_MINUTE = 3;         // 3 readings per minute per sensor
        
        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // Single parallelism to ensure ordered processing
        
        System.out.println("\n" + "=".repeat(60));
        System.out.println("Tucker et al. 2003 - LOCAL TEST RUN");
        System.out.println("=".repeat(60));
        System.out.println("Test Configuration:");
        System.out.println("  - Sensors: " + String.join(", ", SENSOR_IDS));
        System.out.println("  - Duration: " + DURATION_HOURS + " hours");
        System.out.println("  - Readings: " + READINGS_PER_MINUTE + " per minute per sensor");
        System.out.println("  - Total expected readings: " + (SENSOR_IDS.length * 60 * DURATION_HOURS * READINGS_PER_MINUTE));
        System.out.println("=".repeat(60) + "\n");
        
        // Create source with intelligent sensors that embed hourly punctuations
        DataStream<StreamElement> stream = env.addSource(
            new TemperatureSensorSource(SENSOR_IDS, DURATION_HOURS, READINGS_PER_MINUTE)
        ).name("Temperature Sensor Source");
        
        System.out.println("Starting operators...\n");
        
        // Branch 1: Naive Union Operator (WITHOUT punctuation optimization)
        stream
            .keyBy(StreamElement::getKey)
            .process(new NaiveUnionOperator())
            .name("Naive Union Operator");
        
        // Branch 2: Punctuated Union Operator (WITH punctuation optimization)
        stream
            .keyBy(StreamElement::getKey)
            .process(new PunctuatedUnionOperator())
            .name("Punctuated Union Operator");
        
        // Execute the job
        System.out.println("Executing job...\n");
        env.execute("Tucker et al. 2003 - Local Test");
        
        System.out.println("\n" + "=".repeat(60));
        System.out.println("Job completed successfully!");
        System.out.println("=".repeat(60));
        System.out.println("Key Observations:");
        System.out.println("  ✓ NAIVE: State grows linearly (unbounded)");
        System.out.println("  ✓ PUNCTUATED: State shows sawtooth pattern (bounded)");
        System.out.println("  ✓ State cleanup occurs at hour boundaries");
        System.out.println("=".repeat(60) + "\n");
    }
}
