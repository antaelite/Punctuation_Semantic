package org.example.punctuation;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.model.Punctuation;
import org.example.model.StreamElement;

/**
 * Injects punctuation when a specified field value changes.
 *
 * <p>
 * <b>Tucker Semantics:</b> Emits punctuation to signal "no more data with the
 * previous field value". This enables bounded state in downstream operators.
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
}
