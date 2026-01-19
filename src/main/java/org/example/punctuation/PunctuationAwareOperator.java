package org.example.punctuation;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.model.Punctuation;
import org.example.model.StreamElement;

/**
 * Abstract base class for operators that handle punctuation.
 *
 * <p>
 * Subclasses implement business logic in {@link #processData} and optionally
 * override {@link #onPunctuation} for custom state cleanup.
 *
 * @param <K> Type of the key
 * @param <T> Type of data elements (must extend StreamElement)
 * @param <OUT> Type of output elements
 */
public abstract class PunctuationAwareOperator<K, T extends StreamElement, OUT>
        extends KeyedProcessFunction<K, T, OUT> {

    @Override
    public final void processElement(T element, Context ctx, Collector<OUT> out)
            throws Exception {

        if (element.isPunctuation()) {
            onPunctuation((Punctuation) element, ctx, out);
        } else {
            processData(element, ctx, out);
        }
    }

    /**
     * Process a data element (non-punctuation).
     *
     * @param data The data element
     * @param ctx The context
     * @param out The collector for output
     */
    protected abstract void processData(T data, Context ctx, Collector<OUT> out)
            throws Exception;

    /**
     * Handle a punctuation element. Default behavior: do nothing. Override to
     * implement custom cleanup logic (e.g., clear state).
     *
     * @param punctuation The punctuation element
     * @param ctx The context
     * @param out The collector for output
     */
    protected void onPunctuation(Punctuation punctuation, Context ctx, Collector<OUT> out)
            throws Exception {
        // Default: no-op. Subclasses can override for state cleanup.
    }
}
