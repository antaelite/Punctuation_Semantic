package org.example.punctuation.job;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.punctuation.model.StreamElement;
import org.example.punctuation.operators.PunctuationAwareAggregator;
import org.example.punctuation.source.RandomEventSource;

public class PunctuationSemanticJob {
    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1. Source: Generates DataEvents and Punctuations for keys A, B, C
        DataStream<StreamElement> stream = env.addSource(new RandomEventSource());

        // 2. Process: KeyBy Key -> Aggregate based on Punctuation
        // We KeyBy the "key" field.
        stream
            .keyBy(StreamElement::getKey)
            .process(new PunctuationAwareAggregator())
            .print();

        // Execute the job
        env.execute("Punctuation Semantics Job");
    }
}
