package org.example.punctuation.operators;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.punctuation.model.DataEvent;
import org.example.punctuation.model.Punctuation;
import org.example.punctuation.model.StreamElement;

/**
 * A "Blocking" operator that aggregates data.
 * It uses Punctuation to unblock and purge state.
 *
 * Implements Tucker et al. invariants:
 * - PASS: Output result when Punctuation arrives (covering the key).
 * - KEEP: Purge state when Punctuation arrives.
 */
public class PunctuationAwareAggregator extends KeyedProcessFunction<String, StreamElement, String> {

    // Buffer to hold data until punctuation arrives
    private ListState<DataEvent> bufferState;

    @Override
    public void open(Configuration parameters) throws Exception {
        bufferState = getRuntimeContext().getListState(new ListStateDescriptor<>("buffer", DataEvent.class));
    }

    @Override
    public void processElement(StreamElement element, Context ctx, Collector<String> out) throws Exception {
        if (element.isPunctuation()) {
            Punctuation punc = (Punctuation) element;
            System.out.println("OPERATOR: Received Punctuation for key: " + ctx.getCurrentKey());

            // --- PASS INVARIANT Logic ---
            // The Punctuation asserts "No more data for this key".
            // So we can safely process all buffered data for this key.
            
            List<DataEvent> events = new ArrayList<>();
            for (DataEvent e : bufferState.get()) {
                events.add(e);
            }

            if (!events.isEmpty()) {
                // Perform the Aggregation (e.g., Count/Collect)
                String result = "Result for Key " + ctx.getCurrentKey() + ": Processed " + events.size() + " events. Payload: " + events;
                out.collect(result);
            }

            // --- KEEP INVARIANT Logic ---
            // The Punctuation covers all data for this key (in this simplified model).
            // So we don't need to keep any state for this key anymore.
            bufferState.clear();
            System.out.println("OPERATOR: State cleared for key: " + ctx.getCurrentKey());

        } else {
            // It's a DataEvent
            DataEvent event = (DataEvent) element;
            System.out.println("OPERATOR: Buffering event for key: " + ctx.getCurrentKey());
            bufferState.add(event);
        }
    }
}
