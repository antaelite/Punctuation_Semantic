package org.example.operators;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serial;

/**
 * Aggregates wait times by borough to compute average.
 *
 * <p>
 * <b>Input:</b> {@code Tuple3<String borough, Long waitTime, Integer count>}
 * <br><b>Output:</b> {@code Tuple2<String borough, Double averageWaitTime>}
 *
 * <p>
 * Maintains running sum and count per borough to calculate average wait time.
 */
public class AverageAggregationOperator
        extends KeyedProcessFunction<String, Tuple3<String, Long, Integer>, Tuple2<String, Double>> {

    private transient ValueState<AggregateInfo> aggregateState;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<AggregateInfo> descriptor = new ValueStateDescriptor<>(
                "borough-aggregate",
                TypeInformation.of(new TypeHint<>() {
                }));
        aggregateState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(
            Tuple3<String, Long, Integer> input,
            Context ctx,
            Collector<Tuple2<String, Double>> out) throws Exception {

        String borough = input.f0;
        long waitTime = input.f1;
        int count = input.f2;

        AggregateInfo current = aggregateState.value();
        if (current == null) {
            current = new AggregateInfo();
        }

        current.totalWaitTime += waitTime;
        current.totalCount += count;
        aggregateState.update(current);

        double average = (double) current.totalWaitTime / current.totalCount;
        out.collect(new Tuple2<>(borough, average));

        System.out.println("AGG [" + borough + "]: "
                + "Count=" + current.totalCount
                + ", AvgWait=" + String.format("%.1f", average) + "s");
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
