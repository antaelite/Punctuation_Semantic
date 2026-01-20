package org.example.app;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.model.StreamElement;
import org.example.model.WaitTimeData;
import org.example.operators.StreamThrottler;
import org.example.operators.TaxiCsvMapper;
import org.example.operators.stateful.GroupByOperator;
import org.example.operators.stateful.TaxiWaitTimeOperator;
import org.example.punctuated.PunctuatedBatchOperator;
import org.example.punctuated.PunctuationInjector;

/**
 * Tucker et al. (2003) Faithful Implementation: Wait Time Between Fares Query
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
        System.err.println("\n***** MAIN TAXI STARTING *****\n");

        // CSV file MUST be sorted by (medallion, dropoff_datetime) for event-time ordering
        String CSV_FILE_PATH = ".\\src\\main\\resources\\nyc_taxi_medallion_sorted.csv";

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
                .name("Parse CSV")
                .process(new StreamThrottler<>(0)) // 10ms delay = 100 records/sec
                .name("Stream Throttler");

        // Inject punctuation when medallion changes (Tucker: "no more rides from this taxi")
        // NOTE: Must be BEFORE keyBy so injector sees medallion transitions globally!
        DataStream<StreamElement> withPunctuation = csvStream
                .keyBy(new GlobalKeySelector())
                .process(new PunctuationInjector<>("medallion"))
                .name("Inject Punctuation");

        // Punctuated batch operator (Tucker Section 4.2: PASS Invariant - bounded memory)
        // NOW we keyBy medallion to partition data for parallel processing
        DataStream<StreamElement> bufferedStream = withPunctuation
                .keyBy(StreamElement::getKey)
                .process(new PunctuatedBatchOperator())
                .name("Punctuated Batch Operator");

        // Taxi wait time operator (Tucker Section 7: Stateful computation)
        // Tucker: Operators process both data and punctuation, cleaning state on punctuation
        DataStream<StreamElement> waitTimes = bufferedStream
                .keyBy(StreamElement::getKey)
                .process(new TaxiWaitTimeOperator())
                .name("Taxi Wait Time Operator");

        // GroupBy operator (Tucker Section 7.2: Aggregation with bounded memory)
        // Tucker: Final operator also implements KEEP/PASS/Propagation invariants
        DataStream<StreamElement> results = waitTimes
                .keyBy(element -> {
                    if (element instanceof WaitTimeData) {
                        return ((WaitTimeData) element).borough();
                    }
                    return "PUNCTUATION"; // Group all punctuation together
                })
                .process(new GroupByOperator())
                .name("GroupBy Operator");

        // Print results (will show both data and punctuation flow)
        results.print().name("Results");

        // Execute
        System.out.println("Starting Tucker Replication Job...\n");
        env.execute("Tucker 2003 - NYC Taxi Wait Time Analysis");
    }
}
