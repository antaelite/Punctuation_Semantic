package org.example.operators;

import java.util.Iterator;
import java.util.Map;
import java.util.stream.StreamSupport;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.model.Punctuation;
import org.example.model.StreamElement;

/**
 * Unified Punctuated Union Operator (Duplicate Elimination). Works for any
 * StreamElement by using getDeduplicationKey() and Punctuation generic fields.
 */
public class PunctuatedUnionOperator extends KeyedProcessFunction<String, StreamElement, String> {

    // MapState: Key = DeduplicationKey (Object), Value = StreamElement
    private transient MapState<Object, StreamElement> seenElements;

    @Override
    public void open(Configuration parameters) {
        seenElements = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("unified-punctuated-seen", Object.class, StreamElement.class)
        );
    }

    @Override
    public void processElement(StreamElement element, Context ctx, Collector<String> out) throws Exception {

        if (element.isPunctuation()) {
            Punctuation punc = (Punctuation) element;
            String field = punc.getField();
            Object puncVal = punc.getValue();

            System.out.println(">>> PUNCTUATED-UNION [" + ctx.getCurrentKey() + "]: Received Punctuation on '" + field + "' with value " + puncVal);

            Iterator<Map.Entry<Object, StreamElement>> iterator = seenElements.iterator();
            int deletedCount = 0;

            while (iterator.hasNext()) {
                Map.Entry<Object, StreamElement> entry = iterator.next();
                StreamElement storedElement = entry.getValue();

                Object storedVal = storedElement.getValue(field);

                if (storedVal == null) {
                    continue; // Field not present in this element
                }
                boolean shouldRemove = false;

                // Comparison Logic
                if (puncVal instanceof Integer && storedVal instanceof Integer) {
                    // For Integers (like hours), assume ordered property (<=)
                    if ((Integer) storedVal <= (Integer) puncVal) {
                        shouldRemove = true;
                    }
                } else {
                    // For others (Ids, Strings), assume exact match
                    if (storedVal.equals(puncVal)) {
                        shouldRemove = true;
                    }
                }

                if (shouldRemove) {
                    iterator.remove();
                    deletedCount++;
                }
            }

            long remainingSize = StreamSupport.stream(seenElements.keys().spliterator(), false).count();
            System.out.println("PUNCTUATED-UNION: Purged " + deletedCount + " elements in partition " + ctx.getCurrentKey() + ". New State Size = " + remainingSize);

            out.collect("PUNCTUATED-UNION: Cleared " + field + "=" + puncVal + " (Removed " + deletedCount + ")");

        } else {
            // --- DATA LOGIC ---
            Object key = element.getDeduplicationKey();

            if (!seenElements.contains(key)) {
                seenElements.put(key, element);
            }
        }
    }
}
