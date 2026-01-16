package org.example.operators;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.model.Punctuation;
import org.example.model.StreamElement;
import org.example.model.TaxiRide;

import java.io.Serial;

/**
 * Calculates wait time between consecutive fares for the same taxi.
 *
 * <p>
 * Implements Tucker et al. (2003) punctuation semantics to answer the query:
 * "What is the average time it takes for a taxi to find its next fare per
 * destination borough?"
 *
 * <p>
 * <b>Input:</b> Stream of {@link TaxiRide} and {@link Punctuation} objects,
 * keyed by medallion (taxi ID)
 * <br><b>Output:</b>
 * {@code Tuple3<String borough, Long waitTimeSeconds, Integer count>}
 *
 * <p>
 * <b>Tucker Application:</b>
 * <ul>
 * <li>Buffer: Stores last drop-off time/location for each taxi</li>
 * <li>Punctuation: Signals "no more rides from this taxi", allowing state
 * cleanup</li>
 * <li>Bounded memory: Old taxi state is purged when punctuation arrives</li>
 * </ul>
 *
 * @see <a href="https://doi.org/10.1145/776752.776780">Tucker et al. 2003</a>
 */
public class WaitTimeBetweenFaresOperator
        extends KeyedProcessFunction<String, StreamElement, Tuple3<String, Long, Integer>> {

    private transient ValueState<DropOffInfo> lastDropOff;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<DropOffInfo> descriptor = new ValueStateDescriptor<>(
                "last-dropoff",
                TypeInformation.of(new TypeHint<>() {
                }));
        lastDropOff = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(StreamElement element, Context ctx, Collector<Tuple3<String, Long, Integer>> out)
            throws Exception {

        if (element.isPunctuation()) {
            handlePunctuation((Punctuation) element, ctx);
        } else {
            handleTaxiRide((TaxiRide) element, ctx, out);
        }
    }

    private void handleTaxiRide(TaxiRide ride, Context ctx, Collector<Tuple3<String, Long, Integer>> out)
            throws Exception {

        DropOffInfo previous = lastDropOff.value();

        if (previous != null) {
            long waitTimeMillis = ride.getPickupTimestamp() - previous.dropOffTimestamp;
            long waitTimeSeconds = waitTimeMillis / 1000;

            if (waitTimeSeconds >= 0) {
                String borough = previous.borough;
                out.collect(new Tuple3<>(borough, waitTimeSeconds, 1));

                System.out.println(">>> WAIT-TIME [" + ctx.getCurrentKey() + "]: "
                        + "Dropped in " + borough + ", waited " + waitTimeSeconds + "s for next fare");
            }
        }

        DropOffInfo currentDropOff = new DropOffInfo(
                ride.getDropoffTimestamp(),
                ride.getDropoffBorough()
        );
        lastDropOff.update(currentDropOff);
    }

    private void handlePunctuation(Punctuation punctuation, Context ctx) throws Exception {
        String field = punctuation.field();

        if ("medallion".equals(field)) {
            lastDropOff.clear();
            System.out.println(">>> PUNCTUATION [" + ctx.getCurrentKey() + "]: "
                    + "Cleared state for medallion (Tucker: no more rides from this taxi)");
        }

        if ("hour".equals(field)) {
            System.out.println(">>> PUNCTUATION [" + ctx.getCurrentKey() + "]: "
                    + "Hour " + punctuation.value() + " complete");
        }
    }

    /**
     * POJO to store drop-off information in Flink state. Must be Serializable
     * for Flink's state backend.
     */
    public static class DropOffInfo implements java.io.Serializable {

        @Serial
        private static final long serialVersionUID = 1L;

        public long dropOffTimestamp;
        public String borough;

        public DropOffInfo(long dropOffTimestamp, String borough) {
            this.dropOffTimestamp = dropOffTimestamp;
            this.borough = borough;
        }
    }
}
