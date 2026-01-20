package org.example.punctuated;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.model.Punctuation;
import org.example.model.StreamElement;

/**
 * Injects punctuation when a specified field value changes (Tucker Section 5).
 *
 * <p>
 * <b>Tucker Semantics:</b> Emits punctuation to signal "no more data with the
 * previous field value will arrive". This enables bounded state in downstream
 * operators.
 *
 * <p>
 * <b>Example:</b> Inject punctuation when medallion changes → signals end of
 * taxi's rides
 *
 * <p>
 * <b>Usage:</b>
 * <pre>
 * stream.keyBy(StreamElement::getKey)
 *       .process(new PunctuationInjector("medallion"))
 * </pre>
 *
 * @param <T> Type of data elements (must extend StreamElement)
 *
 * @see <a href="https://doi.org/10.1145/776752.776780">Tucker et al. 2003,
 * Section 5</a>
 */
public class PunctuationInjector<T extends StreamElement>
        extends KeyedProcessFunction<String, T, StreamElement> {

    private final String fieldName;
    private transient ValueState<Object> lastValue;

    /**
     * Create a punctuation injector that emits punctuation when the specified
     * field changes.
     *
     * @param fieldName Name of the field to monitor (must be supported by
     * StreamElement.getValue())
     */
    public PunctuationInjector(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Object> descriptor = new ValueStateDescriptor<>(
                "last-" + fieldName,
                Object.class
        );
        lastValue = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(T element, Context ctx, Collector<StreamElement> out)
            throws Exception {

        // Skip punctuation elements - just pass them through
        if (element.isPunctuation()) {
            out.collect(element);
            return;
        }

        Object currentValue = element.getValue(fieldName);
        Object previousValue = lastValue.value();

        // DEBUG: Log first few elements
        if (previousValue == null || !previousValue.equals(currentValue)) {
            System.out.println(">>> INJECTOR [" + ctx.getCurrentKey() + "]: "
                    + "Field=" + fieldName + ", prev=" + previousValue + ", current=" + currentValue);
        }

        // If field value changed, emit punctuation with previous value
        if (previousValue != null && !previousValue.equals(currentValue)) {
            Punctuation punctuation = new Punctuation(
                    element.getKey(),
                    fieldName,
                    previousValue,
                    element.timestamp()
            );

            out.collect(punctuation);

            System.out.println(">>> PUNCTUATION-INJECTOR [" + ctx.getCurrentKey() + "]: "
                    + "Injected punctuation for field=" + fieldName
                    + ", old=" + previousValue + ", new=" + currentValue);
        }

        // Emit the data element
        out.collect(element);

        // Update state
        lastValue.update(currentValue);
    }

    @Override
    public void close() throws Exception {
        // Note: We cannot access state here because close() is called outside of a keyed context
        // The PunctuatedBatchOperator's pass-through emission strategy ensures all data gets processed
        System.out.println(">>> PUNCTUATION-INJECTOR [END-OF-STREAM]: Closing injector");
        super.close();
    }
}
