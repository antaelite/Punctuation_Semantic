package org.example.punctuation.metrics;

/**
 * Container for state size metrics to track memory usage over time.
 * Used to replicate Figure 2(a) from Tucker et al. 2003.
 */
public class StateMetrics {
    private final long timestamp;
    private final int hour;
    private final int minute;
    private final long stateSize;
    private final String operatorType; // "NAIVE" or "PUNCTUATED"

    public StateMetrics(long timestamp, int hour, int minute, long stateSize, String operatorType) {
        this.timestamp = timestamp;
        this.hour = hour;
        this.minute = minute;
        this.stateSize = stateSize;
        this.operatorType = operatorType;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public int getHour() {
        return hour;
    }

    public int getMinute() {
        return minute;
    }

    public long getStateSize() {
        return stateSize;
    }

    public String getOperatorType() {
        return operatorType;
    }

    @Override
    public String toString() {
        return "StateMetrics{" +
                "hour=" + hour +
                ", minute=" + minute +
                ", stateSize=" + stateSize +
                ", operatorType='" + operatorType + '\'' +
                '}';
    }

    /**
     * Format for CSV export to replicate Figure 2(a)
     */
    public String toCsv() {
        return operatorType + "," + hour + "," + minute + "," + stateSize;
    }

    public static String csvHeader() {
        return "operator_type,hour,minute,state_size";
    }
}
