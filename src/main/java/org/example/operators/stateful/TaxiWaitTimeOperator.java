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
import org.example.model.TaxiRide;
import org.example.model.WaitTimeData;

/**
 * Taxi wait time operator - calculates time between consecutive fares (Tucker
 * Section 7).
 *
 * <p>
 * <b>Tucker Terminology:</b> This is a stateful operator that computes derived
 * values (wait times) from consecutive data elements.
 *
 * <p>
 * <b>Input:</b> Stream of {@link StreamElement} objects (TaxiRide +
 * Punctuation), keyed by medallion (taxi ID)
 * <br><b>Output:</b> StreamElement - WaitTimeData for wait times, or forwarded
 * punctuation
 *
 * <p>
 * <b>Tucker Invariants:</b>
 * <ul>
 * <li><b>KEEP Invariant:</b> When medallion punctuation arrives, clear last
 * drop-off state (bounded memory)</li>
 * <li><b>Propagation Invariant:</b> Forward punctuation downstream after state
 * cleanup</li>
 * </ul>
 *
 * @see <a href="https://doi.org/10.1145/776752.776780">Tucker et al. 2003,
 * Section 7</a>
 */
public class TaxiWaitTimeOperator
        extends KeyedProcessFunction<String, StreamElement, StreamElement> {

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
    public void processElement(StreamElement element, Context ctx, Collector<StreamElement> out)
            throws Exception {

        if (element.isPunctuation()) {
            // Handle punctuation
            onPunctuation((Punctuation) element, ctx, out);
        } else {
            // Process data
            processData((TaxiRide) element, ctx, out);
        }
    }

    /**
     * Process a TaxiRide data element.
     */
    protected void processData(TaxiRide ride, Context ctx, Collector<StreamElement> out)
            throws Exception {

        DropOffInfo previous = lastDropOff.value();

        if (previous != null) {
            long waitTimeMillis = ride.getPickupTimestamp() - previous.dropOffTimestamp;
            long waitTimeSeconds = waitTimeMillis / 1000;

            if (waitTimeSeconds >= 0) {
                String borough = previous.borough;
                WaitTimeData waitTimeData = new WaitTimeData(borough, waitTimeSeconds, 1, ctx.getCurrentKey());
                out.collect(waitTimeData);

                System.out.println(">>> TAXI-WAIT-TIME [" + ctx.getCurrentKey() + "]: "
                        + "Dropped in " + borough + ", waited " + waitTimeSeconds + "s for next fare");
            }
        }

        DropOffInfo currentDropOff = new DropOffInfo(
                ride.getDropoffTimestamp(),
                ride.getDropoffBorough()
        );
        lastDropOff.update(currentDropOff);
    }

    /**
     * Handle punctuation - implements Tucker KEEP Invariant. When medallion
     * punctuation arrives, clear state for bounded memory.
     */
    protected void onPunctuation(Punctuation punctuation, Context ctx, Collector<StreamElement> out)
            throws Exception {

        // KEEP Invariant: Clear state when medallion punctuation arrives
        if ("medallion".equals(punctuation.field())) {
            lastDropOff.clear();
            System.out.println(">>> TAXI-WAIT-TIME [" + ctx.getCurrentKey() + "]: "
                    + "KEEP Invariant - Cleared state for medallion=" + punctuation.value());
        }

        // Propagation Invariant: Forward punctuation downstream
        out.collect(punctuation);
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

        // No-arg constructor required for Kryo serialization
        public DropOffInfo() {
        }

        public DropOffInfo(long dropOffTimestamp, String borough) {
            this.dropOffTimestamp = dropOffTimestamp;
            this.borough = borough;
        }
    }
}
