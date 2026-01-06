package org.example.punctuation.job;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.punctuation.model.StreamElement;
import org.example.punctuation.operators.NaiveUnionOperator;
import org.example.punctuation.operators.PunctuatedUnionOperator;
import org.example.punctuation.source.TemperatureSensorSource;

/**
 * Main job replicating Tucker et al. 2003 "Exploiting Punctuation Semantics in Continuous Data Streams"
 * 
 * Demonstrates:
 * 1. Temperature warehouse scenario (Section 7.2.1)
 * 2. Union operator with duplicate elimination
 * 3. Comparison between naive (unbounded state) and punctuated (bounded state) approaches
 * 4. Replication of Figure 2(a): State Size for Union Operator
 * 
 * Expected behavior:
 * - NAIVE: Linear growth of state (memory leak)
 * - PUNCTUATED: Sawtooth pattern with state purged at end of each hour
 */
public class TuckerReplicationJob {
    
    public static void main(String[] args) throws Exception {
        // Configuration matching Tucker et al. 2003 Section 7.3
        final String[] SENSOR_IDS = {"S1", "S2", "S3"}; // Multiple sensors
        final int DURATION_HOURS = 5;  // Simulate 5 hours (paper uses 60, but we use 5 for demo)
        final int READINGS_PER_MINUTE = 2; // Readings frequency
        
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
        
        // Create source with intelligent sensors that embed hourly punctuations
        DataStream<StreamElement> stream = env.addSource(
            new TemperatureSensorSource(SENSOR_IDS, DURATION_HOURS, READINGS_PER_MINUTE)
        );
        
        // Branch 1: Naive Union Operator (WITHOUT punctuation optimization)
        System.out.println("Starting NAIVE Union Operator (unbounded state)...");
        stream
            .keyBy(StreamElement::getKey)
            .process(new NaiveUnionOperator())
            .print()
            .name("Naive Union Output");
        
        // Branch 2: Punctuated Union Operator (WITH punctuation optimization)
        System.out.println("Starting PUNCTUATED Union Operator (bounded state with cleanup)...");
        stream
            .keyBy(StreamElement::getKey)
            .process(new PunctuatedUnionOperator())
            .print()
            .name("Punctuated Union Output");
        
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
