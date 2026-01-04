//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

// package org.apache.flink.streaming.examples.socket;

import java.time.Duration;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.util.ParameterTool;

public class SocketWindowWordCount {
    public static void main(String[] args) throws Exception {
        String hostname;
        int port;
        boolean asyncState;
        try {
            ParameterTool params = ParameterTool.fromArgs(args);
            hostname = params.has("hostname") ? params.get("hostname") : "localhost";
            port = params.getInt("port");
            asyncState = params.has("async-state");
        } catch (Exception var8) {
            System.err.println("No port specified. Please run 'SocketWindowWordCount --hostname <hostname> --port <port> [--asyncState]', where hostname (localhost by default) and port is the address of the text server");
            System.err.println("To start a simple text server, run 'netcat -l <port>' and type the input text into the command line");
            return;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> text = env.socketTextStream(hostname, port, "\n");
        KeyedStream<WordWithCount, String> keyedStream = text.flatMap((value, out) -> {
            for(String word : value.split("\\s")) {
                out.collect(new WordWithCount(word, 1L));
            }

        }, Types.POJO(WordWithCount.class)).keyBy((value) -> value.word);
        if (asyncState) {
            keyedStream = keyedStream.enableAsyncState();
        }

        DataStream<WordWithCount> windowCounts = keyedStream.window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(5L))).reduce((a, b) -> new WordWithCount(a.word, a.count + b.count)).returns(WordWithCount.class);
        windowCounts.print().setParallelism(1);
        env.execute("Socket Window WordCount");
    }

    public static class WordWithCount {
        public String word;
        public long count;

        public WordWithCount() {
        }

        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        public String toString() {
            return this.word + " : " + this.count;
        }
    }
}
