package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.ingestion.PunctuationGenerator;
import org.example.core.StreamItem;
import org.example.operators.StreamDuplicateElimination;
import org.example.ingestion.TaxiDataMapper;

public class Main {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String filePath = "src/main/resources/sample.csv";

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
                .flatMap(new PunctuationGenerator());

        // keyBy and 
        DataStream<StreamItem> processedStream = stream
                .keyBy(item -> "global")
                // For streamDuplicate
                //.process(new StreamDuplicateElimination());
                // For Query 3
                .process(new org.example.operators.Query3Intra());

                // Pour la Query 4
                // .process(new org.example.operators.Query4Inter());

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
