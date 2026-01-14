package org.example.operators;

import java.util.stream.StreamSupport;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.model.StreamElement;

/**
 * Unified Naive Union Operator (Duplicate Elimination). Works for any
 * StreamElement by using getDeduplicationKey().
 *
 * WITHOUT Punctuation optimization: - Keeps ALL unique tuples in state
 * indefinitely
 */
public class NaiveUnionOperator extends KeyedProcessFunction<String, StreamElement, String> {

    // MapState: Key = DeduplicationKey (Object), Value = StreamElement
    private transient MapState<Object, StreamElement> seenElements;

    @Override
    public void open(Configuration parameters) {
        seenElements = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("unified-naive-seen", Object.class, StreamElement.class)
        );
    }

    @Override
    public void processElement(StreamElement element, Context ctx, Collector<String> out) throws Exception {

        if (element.isPunctuation()) {
            // Unbounded Approach: Ignore punctuation
            long stateSize = StreamSupport.stream(seenElements.keys().spliterator(), false).count();
            System.out.println("NAIVE-UNION [" + ctx.getCurrentKey() + "]: State Size = " + stateSize);

        } else {
            // It's a data element
            Object key = element.getDeduplicationKey();

            if (!seenElements.contains(key)) {
                seenElements.put(key, element);
                // out.collect("NAIVE-UNION: " + element); // Optional output
            }
        }
    }
}
