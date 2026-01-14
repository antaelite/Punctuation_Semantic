package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.model.StreamElement;
import org.example.operators.PunctuatedUnionOperator;
import org.example.source.TaxiPunctuationMapper;

/**
 * Main job replicating Tucker et al. 2003 with NYC Taxi Dataset. * Scenario:
 * Query 3 (Trips per hour analysis) - Source: Real-world CSV Data (NYC Taxi) -
 * Punctuation: Injected by Source based on CSV timestamps - Comparison: Naive
 * (Unbounded State) vs Punctuated (Bounded State)
 */
public class MainTaxi {

    public static void main(String[] args) throws Exception {
        // Path to the CSV file
        String CSV_FILE_PATH = "./src/main/resources/nyc_taxi_punctuated.csv";

        // Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // Single thread to respect CSV chronological order

        System.out.println("Punctuation Semantics - NYC Taxi");
        System.out.println("Dataset: " + CSV_FILE_PATH);

        // Prepare the FileSource definition
        FileSource<String> fileSource = FileSource.forRecordStreamFormat(
                new TextLineInputFormat(), new Path(CSV_FILE_PATH))
                .build();

        // --- PUNCTUATED STREAM ---
        // Create second independent stream for the optimized operator
        DataStream<StreamElement> stream2 = env.fromSource(
                fileSource,
                WatermarkStrategy.noWatermarks(),
                "Taxi Source (Punctuated)")
                .flatMap(new TaxiPunctuationMapper())
                .name("Taxi Mapper (Punctuated)");

        // --- PUNCTUATED OPERATOR ---
        System.out.println("Configuring PUNCTUATED Operator pipeline...");
        DataStream<String> punctuatedOutput = stream2
                // Same grouping key
                .keyBy(element -> "NYC-TAXI")
                .process(new PunctuatedUnionOperator())
                .name("Punctuated Operator (Bounded)");

        punctuatedOutput.print().name("Punctuated-Sink");

        // --- EXECUTION ---
        System.out.println("Starting Job...");
        env.execute("Tucker Replication - NYC Taxi");
    }
}
