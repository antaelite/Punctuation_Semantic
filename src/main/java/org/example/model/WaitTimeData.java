package org.example.model;

import java.io.Serial;

/**
 * Represents wait time data between consecutive fares. Implements StreamElement
 * to allow mixing with punctuation in streams.
 *
 * <p>
 * <b>Tucker et al. 2003 Context:</b> This is a derived data element
 * representing computed wait times between consecutive taxi fares, grouped by
 * borough.
 *
 * <p>
 * <b>Note:</b> Converted from record to POJO for Kryo serialization
 * compatibility.
 */
public class WaitTimeData implements StreamElement {

    @Serial
    private static final long serialVersionUID = 1L;

    private final String borough;
    private final long waitTimeSeconds;
    private final int count;
    private final String medallion;

    /**
     * No-arg constructor required for Kryo serialization.
     */
    public WaitTimeData() {
        this.borough = "";
        this.waitTimeSeconds = 0L;
        this.count = 0;
        this.medallion = "";
    }

    /**
     * Create wait time data.
     *
     * @param borough The borough where the taxi was dropped off
     * @param waitTimeSeconds The wait time in seconds until next fare
     * @param count Count of observations (always 1 for individual rides)
     * @param medallion The taxi ID (medallion)
     */
    public WaitTimeData(String borough, long waitTimeSeconds, int count, String medallion) {
        this.borough = borough;
        this.waitTimeSeconds = waitTimeSeconds;
        this.count = count;
        this.medallion = medallion;
    }

    public String borough() {
        return borough;
    }

    public long waitTimeSeconds() {
        return waitTimeSeconds;
    }

    public int count() {
        return count;
    }

    public String medallion() {
        return medallion;
    }

    @Override
    public boolean isPunctuation() {
        return false;
    }

    @Override
    public String getKey() {
        return medallion;
    }

    @Override
    public long timestamp() {
        return System.currentTimeMillis();
    }

    @Override
    public Object getValue(String field) {
        return switch (field) {
            case "borough" ->
                borough;
            case "medallion" ->
                medallion;
            default ->
                null;
        };
    }

    @Override
    public String toString() {
        return "WaitTimeData{borough='" + borough + "', waitTimeSeconds=" + waitTimeSeconds
                + ", count=" + count + ", medallion='" + medallion + "'}";
    }
}
