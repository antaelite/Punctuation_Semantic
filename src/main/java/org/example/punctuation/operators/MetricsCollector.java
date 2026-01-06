package org.example.punctuation.operators;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.example.punctuation.metrics.StateMetrics;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * Collects state metrics and writes them to a CSV file for analysis.
 * This allows replication of Figure 2(a) from Tucker et al. 2003.
 */
public class MetricsCollector implements SinkFunction<StateMetrics> {
    private static final String OUTPUT_FILE = "state_metrics.csv";
    private static final List<StateMetrics> metrics = new ArrayList<>();
    private static boolean headerWritten = false;

    @Override
    public void invoke(StateMetrics value, Context context) throws Exception {
        synchronized (metrics) {
            metrics.add(value);
            
            // Write to file periodically (every 50 metrics)
            if (metrics.size() >= 50) {
                writeToFile();
            }
        }
    }

    private static void writeToFile() {
        try (PrintWriter writer = new PrintWriter(new FileWriter(OUTPUT_FILE, true))) {
            if (!headerWritten) {
                writer.println(StateMetrics.csvHeader());
                headerWritten = true;
            }
            
            for (StateMetrics metric : metrics) {
                writer.println(metric.toCsv());
            }
            
            metrics.clear();
            System.out.println("Metrics written to " + OUTPUT_FILE);
        } catch (IOException e) {
            System.err.println("Error writing metrics: " + e.getMessage());
        }
    }

    /**
     * Call this at the end of the job to flush remaining metrics
     */
    public static void flush() {
        synchronized (metrics) {
            if (!metrics.isEmpty()) {
                writeToFile();
            }
        }
    }
}
