package org.example.punctuation.operators;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.punctuation.model.SensorReading;
import org.example.punctuation.model.StreamElement;

/**
 * Naive Union Operator (Duplicate Elimination) following Tucker et al. 2003 Section 7.2.1.
 * 
 * WITHOUT Punctuation optimization:
 * - Keeps ALL unique tuples in state indefinitely
 * - This leads to unbounded state growth (memory leak)
 * - Simulates the "problematic" baseline approach
 */
public class NaiveUnionOperator extends KeyedProcessFunction<String, StreamElement, String> {
    
    // State: keeps all unique sensor readings (unbounded growth!)
    private MapState<SensorReading, Boolean> seenTuples;
    
    // Metrics
    private long tupleCount = 0;
    private long stateSize = 0;

    @Override
    public void open(Configuration parameters) throws Exception {
        seenTuples = getRuntimeContext().getMapState(
            new MapStateDescriptor<>("seen-tuples", SensorReading.class, Boolean.class)
        );
    }

    @Override
    public void processElement(StreamElement element, Context ctx, Collector<String> out) throws Exception {
        if (element.isPunctuation()) {
            // Naive approach IGNORES punctuations - does not use them for optimization
            // This is the key difference from the optimized version
            return;
        }

        SensorReading reading = (SensorReading) element;
        tupleCount++;

        // Duplicate elimination: only process if not seen before
        if (!seenTuples.contains(reading)) {
            seenTuples.put(reading, true);
            stateSize++;
            
            // Output the unique reading
            out.collect("NAIVE-UNION: " + reading);
        }

        // Periodic state size reporting (every 100 tuples)
        if (tupleCount % 100 == 0) {
            System.out.println("NAIVE-UNION [" + ctx.getCurrentKey() + "]: State size = " + stateSize + 
                             ", Total tuples processed = " + tupleCount);
        }
    }
}
