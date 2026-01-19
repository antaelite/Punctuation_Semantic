package org.example.operators;

import java.io.Serial;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.example.model.Punctuation;
import org.example.model.StreamElement;
import org.example.model.TaxiRide;
import org.example.punctuation.PunctuationAwareOperator;

/**
 * Calculates wait time between consecutive fares for the same taxi.
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
 */
public class WaitTimeBetweenFaresOperator
        extends PunctuationAwareOperator<String, StreamElement, Tuple3<String, Long, Integer>> {

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
    protected void processData(StreamElement element, Context ctx, Collector<Tuple3<String, Long, Integer>> out)
            throws Exception {

        TaxiRide ride = (TaxiRide) element;
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

    @Override
    protected void onPunctuation(Punctuation punctuation, Context ctx, Collector<Tuple3<String, Long, Integer>> out)
            throws Exception {

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
