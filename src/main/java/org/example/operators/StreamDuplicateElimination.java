package org.example.operators;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;

import org.apache.flink.util.Collector;
import org.example.core.PunctuatedIterator;
import org.example.model.Punctuation;
import org.example.core.StreamItem;
import org.example.model.TaxiRide;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.lang.Math.max;

public class StreamDuplicateElimination extends PunctuatedIterator {

    private MapState<String, TaxiRide> seenTuples;
    private ValueState<Long> maxPunctuationTimestamp;  // Tracks the timestamp of the last punctuation seen

    // Counters for metrics
    private int processedTuples = 0;      // Total number of tuples processed (including late/duplicates)
    private int currentStateSize = 0;     // Current size of the seenTuples map

    // CSV writer for metrics
    private PrintWriter csvWriter;

    @Override
    public void open(Configuration parameters) {
        MapStateDescriptor<String, TaxiRide> descriptor = new MapStateDescriptor<>("seenTuples", String.class, TaxiRide.class);
        seenTuples = getRuntimeContext().getMapState(descriptor);
        maxPunctuationTimestamp = getRuntimeContext().getState(
                new ValueStateDescriptor<>("maxPunctuationTimestamp", Long.class)
        );

        // Initialize CSV writer for metrics
        try {
            int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
            String csvFilename = "analysis/duplicate_elimination_metrics.csv";
            csvWriter = new PrintWriter(new FileWriter(csvFilename, false));
            // Write CSV header
            csvWriter.println("processed_tuples,current_state_size");
            csvWriter.flush();
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize CSV writer", e);
        }
    }

    @Override
    public void step(TaxiRide taxiRide, Context context, Collector<StreamItem> out) throws Exception {
        // Drop data that arrives before the last punctuation timestamp
        Long maxPunctuationTime = maxPunctuationTimestamp.value();

        if (maxPunctuationTime != null && taxiRide.getDropoffTimestamp() <= maxPunctuationTime) {
            System.out.println("LATE DATA DROPPED: " + taxiRide.medallion + " at " + taxiRide.dropoffDatetime);
            processedTuples++;
            writeMetrics();
            return;
        }

        String key = taxiRide.medallion + taxiRide.dropoffDatetime;
        if (!seenTuples.contains(key)) {
            seenTuples.put(key, taxiRide);
            out.collect(taxiRide);
            currentStateSize++;
        } else {
            System.out.println("Duplicate: " + taxiRide.medallion);
        }

        processedTuples++;
        writeMetrics();

    }

    @Override
    public void pass(Punctuation p, Context context, Collector<StreamItem> out) {
        // sdupelim is not a blocking operator, so pass is trivial (returns empty)
    }

    @Override
    public void prop(Punctuation p, Context context, Collector<StreamItem> out) throws Exception {
        // Update the max punctuation timestamp to track the latest punctuation seen
        Long currentMaxPunctuation = maxPunctuationTimestamp.value();
        long newMaxPunctuation = (currentMaxPunctuation != null)
                ? max(p.getEnd(), currentMaxPunctuation)
                : p.getEnd();
        maxPunctuationTimestamp.update(newMaxPunctuation);

        // Propagate the punctuation downstream
        out.collect(p);
    }

    @Override
    public void keep(Punctuation p, Context context) throws Exception {
        // Collect keys to remove
        List<String> keysToRemove = new ArrayList<>();

        for (Map.Entry<String, TaxiRide> entry : seenTuples.entries()) {
            if (p.match(entry.getValue())) {
                keysToRemove.add(entry.getKey());
            }
        }

        // Remove them
        for (String key : keysToRemove) {
            System.out.println("Removed: " + seenTuples.get(key).medallion);
            seenTuples.remove(key);
            currentStateSize--;
        }

        writeMetrics();

    }

    /**
     * Writes current metrics to CSV file
     */
    private void writeMetrics() {
        if (csvWriter != null) {
            csvWriter.println(processedTuples + "," + currentStateSize);
            csvWriter.flush();
        }
    }


    @Override
    public void close() throws Exception {
        if (csvWriter != null) {
            csvWriter.close();
        }
        super.close();
    }
}
