package org.example.punctuation.operators;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.punctuation.model.HourlyPunctuation;
import org.example.punctuation.model.SensorReading;
import org.example.punctuation.model.StreamElement;

import java.util.ArrayList;
import java.util.List;

/**
 * Punctuated Union Operator (Duplicate Elimination) following Tucker et al. 2003.
 * 
 * WITH Punctuation optimization implementing KEEP and PASS invariants:
 * - KEEP Invariant: When punctuation arrives for hour X, purge all state for hour X
 * - PASS Invariant: Output results when punctuation arrives
 * - This prevents unbounded state growth (sawtooth pattern in Figure 2a)
 */
public class PunctuatedUnionOperator extends KeyedProcessFunction<String, StreamElement, String> {
    
    // State: keeps unique sensor readings (bounded by punctuation cleanup)
    private MapState<SensorReading, Boolean> seenTuples;
    
    // Metrics
    private long tupleCount = 0;
    private long stateSize = 0;
    private long punctuationsReceived = 0;

    @Override
    public void open(Configuration parameters) throws Exception {
        seenTuples = getRuntimeContext().getMapState(
            new MapStateDescriptor<>("seen-tuples", SensorReading.class, Boolean.class)
        );
    }

    @Override
    public void processElement(StreamElement element, Context ctx, Collector<String> out) throws Exception {
        if (element.isPunctuation()) {
            HourlyPunctuation punc = (HourlyPunctuation) element;
            punctuationsReceived++;
            
            System.out.println("PUNCTUATED-UNION [" + ctx.getCurrentKey() + "]: Received punctuation for hour " + 
                             punc.getHour() + " (punctuation #" + punctuationsReceived + ")");
            
            // --- KEEP INVARIANT: Purge state for completed hour ---
            // The punctuation asserts "no more data for (sid, hour) will arrive"
            // Therefore, we can safely remove all tuples matching this (sid, hour)
            
            List<SensorReading> toRemove = new ArrayList<>();
            int removedCount = 0;
            
            for (SensorReading reading : seenTuples.keys()) {
                if (reading.getSid().equals(punc.getSid()) && reading.getHour() == punc.getHour()) {
                    toRemove.add(reading);
                }
            }
            
            for (SensorReading reading : toRemove) {
                seenTuples.remove(reading);
                removedCount++;
                stateSize--;
            }
            
            System.out.println("PUNCTUATED-UNION [" + ctx.getCurrentKey() + "]: Purged " + removedCount + 
                             " tuples. State size now = " + stateSize);
            
            // --- PASS INVARIANT: Output summary result ---
            out.collect("PUNCTUATED-UNION: Completed hour " + punc.getHour() + 
                       " for sensor " + punc.getSid() + 
                       ". Processed " + removedCount + " unique readings. State size: " + stateSize);
            
            return;
        }

        // It's a data tuple
        SensorReading reading = (SensorReading) element;
        tupleCount++;

        // Duplicate elimination: only keep if not seen before
        if (!seenTuples.contains(reading)) {
            seenTuples.put(reading, true);
            stateSize++;
            
            // Output the unique reading
            out.collect("PUNCTUATED-UNION: " + reading);
        }

        // Periodic state size reporting (every 100 tuples)
        if (tupleCount % 100 == 0) {
            System.out.println("PUNCTUATED-UNION [" + ctx.getCurrentKey() + "]: State size = " + stateSize + 
                             ", Total tuples = " + tupleCount + ", Punctuations = " + punctuationsReceived);
        }
    }
}
