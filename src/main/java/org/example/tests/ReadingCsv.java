package org.example.tests;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReadingCsv {

    public static void main(String[] args) {
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
        lines.print();

        try {
            env.execute("Reading CSV File");
            System.exit(0);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
