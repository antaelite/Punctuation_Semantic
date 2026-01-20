package org.example.operators.stateful;

import java.io.Serial;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.model.Punctuation;
import org.example.model.StreamElement;
import org.example.model.WaitTimeData;

/**
 * GroupBy operator with aggregation (Tucker et al. 2003, Section 7.2).
 *
 * <p>
 * <b>Tucker Terminology:</b> This implements a "GroupBy" operator that groups
 * wait times by borough and computes running averages.
 *
 * <p>
 * <b>Input:</b> StreamElement - WaitTimeData or Punctuation
 * <br><b>Output:</b> StreamElement - Aggregated results or forwarded
 * punctuation
 *
 * <p>
 * <b>Tucker Invariants:</b>
 * <ul>
 * <li><b>KEEP Invariant:</b> Clear aggregates when punctuation arrives (bounded
 * memory)</li>
 * <li><b>PASS Invariant:</b> Emit final aggregate when punctuation signals
 * completion</li>
 * <li><b>Propagation Invariant:</b> Forward punctuation downstream after
 * processing</li>
 * </ul>
 *
 * @see <a href="https://doi.org/10.1145/776752.776780">Tucker et al. 2003,
 * Section 7.2</a>
 */
public class GroupByOperator
        extends KeyedProcessFunction<String, StreamElement, StreamElement> {

    private transient ValueState<AggregateInfo> aggregateState;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<AggregateInfo> descriptor = new ValueStateDescriptor<>(
                "groupby-aggregate",
                TypeInformation.of(new TypeHint<>() {
                }));
        aggregateState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(StreamElement element, Context ctx, Collector<StreamElement> out)
            throws Exception {

        if (element.isPunctuation()) {
            onPunctuation((Punctuation) element, ctx, out);
        } else if (element instanceof WaitTimeData) {
            processData((WaitTimeData) element, ctx, out);
        }
    }

    /**
     * Process wait time data - update running aggregates.
     */
    protected void processData(WaitTimeData input, Context ctx, Collector<StreamElement> out)
            throws Exception {

        String borough = input.borough();
        long waitTime = input.waitTimeSeconds();
        int count = input.count();

        AggregateInfo current = aggregateState.value();
        if (current == null) {
            current = new AggregateInfo();
        }

        current.totalWaitTime += waitTime;
        current.totalCount += count;
        aggregateState.update(current);

        double average = (double) current.totalWaitTime / current.totalCount;

        System.out.println("GROUPBY [" + borough + "]: "
                + "Count=" + current.totalCount
                + ", AvgWait=" + String.format("%.1f", average) + "s");
    }

    /**
     * Handle punctuation - implements Tucker PASS and KEEP invariants. Emit
     * final results and clear state when punctuation arrives.
     */
    protected void onPunctuation(Punctuation punctuation, Context ctx, Collector<StreamElement> out)
            throws Exception {

        // PASS Invariant: Emit final aggregate when punctuation signals completion
        AggregateInfo current = aggregateState.value();
        if (current != null && current.totalCount > 0) {
            double average = (double) current.totalWaitTime / current.totalCount;
            System.out.println(">>> GROUPBY-FINAL [" + ctx.getCurrentKey() + "]: "
                    + "PASS Invariant - Final average=" + String.format("%.1f", average)
                    + "s (count=" + current.totalCount + ")");
        }

        // KEEP Invariant: Clear state for bounded memory
        aggregateState.clear();
        System.out.println(">>> GROUPBY [" + ctx.getCurrentKey() + "]: "
                + "KEEP Invariant - Cleared aggregation state");

        // Propagation Invariant: Forward punctuation downstream
        out.collect(punctuation);
    }

    /**
     * Holds running aggregation values. Must be Serializable for Flink state.
     */
    public static class AggregateInfo implements java.io.Serializable {

        @Serial
        private static final long serialVersionUID = 1L;

        public long totalWaitTime = 0;
        public int totalCount = 0;

        public AggregateInfo() {
        }
    }
}
