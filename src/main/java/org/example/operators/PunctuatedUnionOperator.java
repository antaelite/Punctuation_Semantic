
package org.example.operators;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.StreamSupport;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.model.HourlyPunctuation;
import org.example.model.SensorReading;
import org.example.model.StreamElement;

/**
 * Punctuated Union Operator (Duplicate Elimination) following Tucker et al. 2003.
 * WITH Punctuation optimization implementing KEEP and PASS invariants:
 * - KEEP Invariant: When punctuation arrives for hour X, purge all state for hour X
 * - PASS Invariant: Output results when punctuation arrives
 * - This prevents unbounded state growth (sawtooth pattern in Figure 2a)
 */
public class PunctuatedUnionOperator extends KeyedProcessFunction<String, StreamElement, String> {

    private MapState<SensorReading, Boolean> seenTuples;
    private long tupleCount = 0;
    private long punctuationsReceived = 0;

    // Metrics for monitoring
    private Counter tuplesProcessed;


    @Override
    public void open(Configuration parameters) {
        seenTuples = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("seen-tuples-punctuated", SensorReading.class, Boolean.class)
        );

        // Register metrics
        tuplesProcessed = getRuntimeContext()
                .getMetricGroup()
                .counter("tuples_processed");

        getRuntimeContext()
                .getMetricGroup()
                .gauge("state_size", () -> {
                    try {
                        return getStateSize();
                    } catch (Exception e) {
                        return -1L;
                    }
                });
    }

    private long getStateSize() throws Exception {
        return StreamSupport.stream(seenTuples.keys().spliterator(), false).count();
    }

    @Override
    public void processElement(StreamElement element, Context ctx, Collector<String> out) throws Exception {
        if (element.isPunctuation()) {
            HourlyPunctuation punc = (HourlyPunctuation) element;
            punctuationsReceived++;

            long beforeSize = getStateSize();
            System.out.println("PUNCTUATED-UNION [" + ctx.getCurrentKey() + "]: Received punctuation for hour " +
                    punc.getHour() + " (punctuation #" + punctuationsReceived + ")");

            // --- KEEP INVARIANT: Purge state for completed hour ---
            // The punctuation asserts "no more data for (sid, hour) will arrive"
            // Therefore, we can safely remove all tuples matching this (sid, hour)
            List<SensorReading> toRemove = new ArrayList<>();

            for (SensorReading reading : seenTuples.keys()) {
                if (reading.getSid().equals(punc.getSid()) && reading.getHour() == punc.getHour()) {
                    toRemove.add(reading);
                }
            }

            for (SensorReading reading : toRemove) {
                seenTuples.remove(reading);
            }

            long afterSize = getStateSize();
            int removedCount = (int)(beforeSize - afterSize);

            System.out.println("PUNCTUATED-UNION [" + ctx.getCurrentKey() + "]: Purged " + removedCount +
                    " tuples. State size now = " + afterSize);

            // --- PASS INVARIANT: Output summary result ---
            out.collect("PUNCTUATED-UNION: Completed hour " + punc.getHour() +
                    " for sensor " + punc.getSid() +
                    ". Processed " + removedCount + " unique readings. State size: " + afterSize);

            return;
        }

        // It's a data tuple
        SensorReading reading = (SensorReading) element;
        tupleCount++;
        tuplesProcessed.inc();

        // Duplicate elimination: only keep if not seen before
        if (!seenTuples.contains(reading)) {
            seenTuples.put(reading, true);

            // Output the unique reading
            out.collect("PUNCTUATED-UNION: " + reading);
        }

        // Periodic state size reporting (every 100 tuples)
        if (tupleCount % 100 == 0) {
            long currentStateSize = getStateSize();
            System.out.println("PUNCTUATED-UNION [" + ctx.getCurrentKey() + "]: State size = " + currentStateSize +
                    ", Total tuples = " + tupleCount + ", Punctuations = " + punctuationsReceived);
        }
    }
}