package org.example.punctuation;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.model.Punctuation;
import org.example.model.StreamElement;
import org.example.model.TaxiRide;

/**
 * Punctuation buffer implementing Tucker et al. (2003) semantics.
 *
 * <p>
 * <b>Behavior:</b>
 * <ul>
 * <li>Data elements are buffered in state</li>
 * <li>When punctuation arrives, all buffered data is emitted as a batch</li>
 * <li>State is cleared after emission (bounded memory)</li>
 * </ul>
 *
 * <p>
 * <b>Input:</b> Stream of {@link StreamElement} (data + punctuation), keyed by
 * partition key
 * <br><b>Output:</b> TaxiRide objects emitted in batches on punctuation
 * boundaries
 */
public class PunctuationBuffer
        extends KeyedProcessFunction<String, StreamElement, TaxiRide> {

    private transient ListState<TaxiRide> buffer;

    @Override
    public void open(Configuration parameters) {
        ListStateDescriptor<TaxiRide> descriptor = new ListStateDescriptor<>(
                "punctuation-buffer",
                TaxiRide.class
        );
        buffer = getRuntimeContext().getListState(descriptor);
    }

    @Override
    public void processElement(StreamElement element, Context ctx, Collector<TaxiRide> out)
            throws Exception {

        if (element.isPunctuation()) {
            // Emit all buffered data
            for (TaxiRide bufferedElement : buffer.get()) {
                out.collect(bufferedElement);
            }

            // Clear the buffer (bounded memory)
            buffer.clear();

            Punctuation p = (Punctuation) element;
            System.out.println(">>> PUNCTUATION-BUFFER [" + ctx.getCurrentKey() + "]: "
                    + "Emitted batch, cleared state for field=" + p.field() + ", value=" + p.value());
        } else {
            // Buffer the data element
            buffer.add((TaxiRide) element);
        }
    }
}
