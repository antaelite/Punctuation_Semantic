package org.example.punctuation.source;

import java.util.Random;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.example.punctuation.model.DataEvent;
import org.example.punctuation.model.Punctuation;
import org.example.punctuation.model.StreamElement;

/**
 * Generates specific keys A, B, C.
 * Eventually sends a Punctuation for a key and stops producing it.
 */
public class RandomEventSource implements SourceFunction<StreamElement> {
    private volatile boolean isRunning = true;
    private final Random random = new Random();

    // Track active keys. true = active, false = finished (punctuation sent)
    private boolean aActive = true;
    private boolean bActive = true;
    private boolean cActive = true;

    @Override
    public void run(SourceContext<StreamElement> ctx) throws Exception {
        int count = 0;
        
        while (isRunning && (aActive || bActive || cActive)) {
            Thread.sleep(500); // Slow down for demo

            String key = pickRandomActiveKey();
            if (key == null) break; // All done

            long timestamp = System.currentTimeMillis();

            // 10% chance to send Punctuation for this key
            if (random.nextInt(10) == 0) {
                ctx.collect(new Punctuation(key, timestamp));
                markKeyFinished(key);
                System.out.println("SOURCE: emitted Punctuation for " + key);
            } else {
                ctx.collect(new DataEvent(key, "Value-" + count++, timestamp));
                System.out.println("SOURCE: emitted Data for " + key);
            }
        }
    }

    private String pickRandomActiveKey() {
        // Simple weighted logic or retry logic
        while (aActive || bActive || cActive) {
            int k = random.nextInt(3);
            if (k == 0 && aActive) return "A";
            if (k == 1 && bActive) return "B";
            if (k == 2 && cActive) return "C";
        }
        return null;
    }

    private void markKeyFinished(String key) {
        if ("A".equals(key)) aActive = false;
        if ("B".equals(key)) bActive = false;
        if ("C".equals(key)) cActive = false;
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
