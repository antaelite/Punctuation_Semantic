package org.apache.flink.research.punctuation.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.research.punctuation.core.PunctuatedRecord;
import java.util.Random;
import java.util.Set;
import java.util.HashSet;

/**
 * A synthetic source simulating the "Outage Tracking" example from the paper.
 * It generates reports (integers) for different IDs.
 * Randomly emits "End-of-ID" punctuations.
 *
 * Enforces the Punctuation Invariant: Once Punctuation(K) is emitted, 
 * strictly NO future Data(K) is generated.
 */
public class PunctuatedGeneratorSource implements SourceFunction<PunctuatedRecord<Integer, Integer>> {

    private volatile boolean isRunning = true;
    private final int numKeys;
    private final Set<Integer> activeKeys;
    private final Random random;

    public PunctuatedGeneratorSource(int numKeys) {
        this.numKeys = numKeys;
        this.activeKeys = new HashSet<>();
        for (int i = 0; i < numKeys; i++) {
            activeKeys.add(i);
        }
        this.random = new Random();
    }

    @Override
    public void run(SourceContext<PunctuatedRecord<Integer, Integer>> ctx) throws Exception {
        while (isRunning &&!activeKeys.isEmpty()) {
            synchronized (ctx.getCheckpointLock()) {
                // Select a random active key
                Integer key = activeKeys.stream()
                   .skip(random.nextInt(activeKeys.size()))
                   .findFirst()
                   .orElse(null);

                if (key == null) break;

                // 5% chance to emit a punctuation (Close the subset)
                if (random.nextDouble() < 0.05) {
                    long ts = System.currentTimeMillis();
                    // Emit Punctuation
                    ctx.collect(new PunctuatedRecord<>(key, ts));
                    System.out.println("SOURCE: Emitted Punctuation for Key " + key);
                    
                    // Enforce source integrity: Remove from active set
                    activeKeys.remove(key);
                } else {
                    // Emit Data
                    long ts = System.currentTimeMillis();
                    int payload = random.nextInt(100);
                    ctx.collect(new PunctuatedRecord<>(payload, ts)); // The 'data' implies the key in this simplified model
                }
                
                // Advance Watermark to keep Flink happy (though we rely on data punctuations)
                ctx.emitWatermark(new Watermark(System.currentTimeMillis() - 100));
            }
            Thread.sleep(10); // Throttle generation
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}