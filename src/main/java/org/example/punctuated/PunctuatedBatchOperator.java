package org.example.punctuated;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.model.Punctuation;
import org.example.model.StreamElement;
import org.example.model.TaxiRide;

/**
 * Punctuated Batch Operator implementing Tucker et al. (2003) batching
 * semantics.
 *
 * <p>
 * <b>Tucker Terminology:</b> This is a "punctuated operator" that batches data
 * elements and emits them as a group when punctuation arrives.
 *
 * <p>
 * <b>Tucker Invariants Implemented:</b>
 * <ul>
 * <li><b>PASS Invariant:</b> Data is emitted ONLY when punctuation arrives
 * (batched emission)</li>
 * <li><b>KEEP Invariant:</b> Buffer is cleared after emission (bounded
 * memory)</li>
 * <li><b>Propagation Invariant:</b> Punctuation is forwarded downstream after
 * processing</li>
 * </ul>
 *
 * <p>
 * <b>Input:</b> Stream of {@link StreamElement} (data + punctuation), keyed by
 * partition key
 * <br><b>Output:</b> StreamElement - TaxiRide objects emitted in batches on
 * punctuation boundaries, plus forwarded punctuation markers
 *
 * @see <a href="https://doi.org/10.1145/776752.776780">Tucker et al. 2003,
 * Section 4.2</a>
 */
public class PunctuatedBatchOperator
        extends KeyedProcessFunction<String, StreamElement, StreamElement> {

    private transient ListState<TaxiRide> buffer;

    @Override
    public void open(Configuration parameters) {
        ListStateDescriptor<TaxiRide> descriptor = new ListStateDescriptor<>(
                "punctuated-batch-buffer",
                TaxiRide.class
        );
        buffer = getRuntimeContext().getListState(descriptor);
    }

    @Override
    public void processElement(StreamElement element, Context ctx, Collector<StreamElement> out)
            throws Exception {

        if (element.isPunctuation()) {
            // PASS Invariant: Emit all buffered data when punctuation arrives
            int emitted = 0;
            for (TaxiRide bufferedElement : buffer.get()) {
                out.collect(bufferedElement);
                emitted++;
            }

            // KEEP Invariant: Clear the buffer (bounded memory!)
            buffer.clear();

            Punctuation p = (Punctuation) element;
            System.out.println(">>> PUNCTUATED-BATCH [" + ctx.getCurrentKey() + "]: "
                    + "PASS Invariant - Emitted " + emitted + " buffered elements. "
                    + "KEEP Invariant - Cleared state for field=" + p.field() + ", value=" + p.value());

            // Propagation Invariant: Forward punctuation downstream
            out.collect(element);
        } else {
            // Tucker semantics: Buffer data, DO NOT emit yet
            // Data will be emitted when punctuation arrives (PASS Invariant)
            TaxiRide ride = (TaxiRide) element;
            buffer.add(ride);
        }
    }

    @Override
    public void close() throws Exception {
        // Note: We cannot access state here because close() is called outside of a keyed context
        // Tucker semantics: Data buffered without punctuation remains buffered
        // (In production, you'd emit on stream end, but Tucker focuses on punctuation-driven emission)
        System.out.println(">>> PUNCTUATED-BATCH [END-OF-STREAM]: Closing batch operator");
        super.close();
    }
}
