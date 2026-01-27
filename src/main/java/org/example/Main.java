package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.ingestion.PunctuationInjector;
import org.example.core.StreamItem;
import org.example.operators.StreamCountSameBorough;
import org.example.operators.StreamCountDiffBorough;
import org.example.ingestion.TaxiDataMapper;
import org.example.operators.StreamDuplicateElimination;
import org.example.operators.StreamGroupBy;

public class Main {

    public static void main(String[] args)  {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String filePath = "src/main/resources/sample_dropofftime.csv";

        // reads file
        FileSource<String> source = FileSource.forRecordStreamFormat(
                new TextLineInputFormat(),
                new Path(filePath)
        ).build();

        // creates a data stream from the file
        DataStream<String> lines = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "text-file-source"
        );

        // transforms the data stream into a stream of StreamItem objects
        DataStream<StreamItem> stream = lines
                .map(new TaxiDataMapper())
                .flatMap(new PunctuationInjector());

        // keyBy and process stream
        DataStream<StreamItem> processedStream = stream
                .keyBy(item -> "global") // forces Flink to send all StreamItems to the same partition
                // For streamDuplicate
                .process(new StreamDuplicateElimination());
                // To count the number of rides in the same borough
//                .process(new StreamCountSameBorough());
                // To count the number of rides in different boroughs
//                 .process(new StreamCountDiffBorough());
                // To group by
//                .process(new StreamGroupBy());

        // print processed stream
        processedStream.print();

        try {
            env.execute("Replication");
            System.exit(0);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
