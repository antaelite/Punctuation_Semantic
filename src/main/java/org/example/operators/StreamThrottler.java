package org.example.operators;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Slows down stream processing to emulate real-world data arrival. Adds a delay
 * between each element to make output visible and simulate gradual data
 * arrival.
 */
public class StreamThrottler<T> extends ProcessFunction<T, T> {

    private final long delayMillis;
    private int count = 0;

    /**
     * Create a throttler with specified delay between elements.
     *
     * @param delayMillis Milliseconds to wait between elements
     */
    public StreamThrottler(long delayMillis) {
        this.delayMillis = delayMillis;
    }

    @Override
    public void processElement(T element, Context ctx, Collector<T> out) throws Exception {
        // Add delay to slow down processing
        if (delayMillis > 0) {
            Thread.sleep(delayMillis);
        }

        out.collect(element);

        count++;
        if (count % 100 == 0) {
            System.out.println(">>> THROTTLER: Processed " + count + " elements");
        }
    }
}
