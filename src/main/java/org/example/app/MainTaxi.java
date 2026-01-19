package org.example.app;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.model.StreamElement;
import org.example.model.TaxiRide;
import org.example.operators.AverageAggregationOperator;
import org.example.operators.TaxiCsvMapper;
import org.example.operators.WaitTimeBetweenFaresOperator;
import org.example.punctuation.PunctuationBuffer;
import org.example.punctuation.PunctuationInjector;

/**
 * Tucker et al. (2003) Replication: Wait Time Between Fares Query
 *
 * <p>
 * <b>Query:</b> What is the average time it takes for a taxi to find its next
 * fare, per destination borough?
 *
 * <p>
 * <b>Tucker Semantics Applied:</b>
 * <ul>
 * <li>Data sorted by (medallion, dropoff_time) - when data is reported</li>
 * <li>Punctuations mark medallion boundaries ("no more rides from taxi X") -
 * injected by operators</li>
 * <li>Bounded state: old taxi data purged when punctuation arrives</li>
 * </ul>
 *
 * <p>
 * <b>Pipeline:</b>
 * <pre>
 * CSV Source → Parse → Key by Medallion → Calculate Wait Times → Aggregate by Borough → Print
 * </pre>
 *
 * @see <a href="https://doi.org/10.1145/776752.776780">Tucker et al. 2003</a>
 */
public class MainTaxi {

    public static void main(String[] args) throws Exception {
        // CSV file MUST be sorted by (medallion, dropoff_datetime) for event-time ordering
        String CSV_FILE_PATH = "./src/main/resources/nyc_taxi_medallion_sorted.csv";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // Single thread to respect CSV ordering

        System.out.println("===========================================");
        System.out.println("Tucker et al. 2003 - Wait Time Query");
        System.out.println("Dataset: " + CSV_FILE_PATH);
        System.out.println("===========================================\n");

        // Read CSV file
        FileSource<String> fileSource = FileSource.forRecordStreamFormat(
                new TextLineInputFormat(),
                new Path(CSV_FILE_PATH))
                .build();

        // Parse CSV into TaxiRide objects
        DataStream<StreamElement> csvStream = env.fromSource(
                fileSource,
                WatermarkStrategy.noWatermarks(),
                "Taxi CSV Source")
                .flatMap(new TaxiCsvMapper())
                .name("Parse CSV");

        // Inject punctuation when medallion changes (Tucker: "no more rides from this taxi")
        DataStream<StreamElement> withPunctuation = csvStream
                .keyBy(StreamElement::getKey)
                .process(new PunctuationInjector<>("medallion"))
                .name("Inject Punctuation");

        // Buffer data and emit on punctuation (Tucker: bounded memory)
        DataStream<TaxiRide> stream = withPunctuation
                .keyBy(StreamElement::getKey)
                .process(new PunctuationBuffer())
                .returns(TaxiRide.class) // Explicit type for Flink type inference
                .name("Punctuation Buffer");

        // Step 1: Calculate wait times per taxi (keyed by medallion)
        // Note: stream is already TaxiRide (buffer filtered out punctuation)
        // We need to wrap it back into StreamElement for the operator
        DataStream<Tuple3<String, Long, Integer>> waitTimes = stream
                .map(ride -> (StreamElement) ride)
                .keyBy(StreamElement::getKey)
                .process(new WaitTimeBetweenFaresOperator())
                .name("Calculate Wait Times");

        // Step 2: Aggregate by borough to get averages
        DataStream<Tuple2<String, Double>> avgByBorough = waitTimes
                .keyBy(tuple -> tuple.f0) // Group by borough
                .process(new AverageAggregationOperator())
                .name("Aggregate by Borough");

        // Print results
        avgByBorough.print().name("Results");

        // Execute
        System.out.println("Starting Tucker Replication Job...\n");
        env.execute("Tucker 2003 - NYC Taxi Wait Time Analysis");
    }
}
