
package org.example.operators;


import java.util.stream.StreamSupport;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.model.SensorReading;
import org.example.model.StreamElement;

/**
 * Naive Union Operator (Duplicate Elimination) following Tucker et al. 2003 Section 7.2.1.
 * <p>
 * WITHOUT Punctuation optimization:
 * - Keeps ALL unique tuples in state indefinitely
 * - Ignores punctuations completely
 * - This leads to unbounded state growth (memory leak)
 * - Simulates the "problematic" baseline approach
 */
public class NaiveUnionOperator extends KeyedProcessFunction<String, StreamElement, String> {

    // State: keeps all unique sensor readings (unbounded growth!)
    private MapState<SensorReading, Boolean> seenTuples;

    // Metrics
    private long tupleCount = 0;
    private long punctuationsReceived = 0;

    @Override
    public void open(Configuration parameters) {
        seenTuples = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("seen-tuples-naive", SensorReading.class, Boolean.class)
        );
    }

    private long getStateSize() throws Exception {
        return StreamSupport.stream(seenTuples.keys().spliterator(), false).count();
    }

    @Override
    public void processElement(StreamElement element, Context ctx, Collector<String> out) throws Exception {
        if (element.isPunctuation()) {
            punctuationsReceived++;
            // NAIVE APPROACH: Ignore punctuations completely - DO NOT clean state!
            System.out.println("NAIVE-UNION [" + ctx.getCurrentKey() + "]: Received punctuation #" +
                    punctuationsReceived + " (IGNORED - state NOT cleaned)");
            return;
        }

        // It's a data tuple
        SensorReading reading = (SensorReading) element;
        tupleCount++;

        // Duplicate elimination: only keep if not seen before
        if (!seenTuples.contains(reading)) {
            seenTuples.put(reading, true);

            // Output the unique reading
            out.collect("NAIVE-UNION: " + reading);
        }

        // Periodic state size reporting (every 100 tuples)
        if (tupleCount % 100 == 0) {
            long currentStateSize = getStateSize();
            System.out.println("NAIVE-UNION [" + ctx.getCurrentKey() + "]: State size = " + currentStateSize +
                    " (GROWING UNBOUNDED), Total tuples = " + tupleCount +
                    ", Punctuations ignored = " + punctuationsReceived);
        }
    }
}