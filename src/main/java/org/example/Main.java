package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.framework.PunctuationGenerator;
import org.example.model.StreamItem;
import org.example.operators.StreamDuplicateElimination;
import org.example.source.TaxiDataSource;

public class Main {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String filePath = "src/main/resources/sample.csv";
        FileSource<String> source = FileSource.forRecordStreamFormat(
                new TextLineInputFormat(),
                new Path(filePath)
        ).build();
        DataStream<String> lines = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "text-file-source"
        );
        DataStream<StreamItem> stream = lines
                .map(new TaxiDataSource())
                .flatMap(new PunctuationGenerator());
        DataStream<StreamItem> processedStream = stream
                .keyBy(item -> "global")
                .process(new StreamDuplicateElimination());
//        processedStream.print();
        env.execute("replication");
    }
}
