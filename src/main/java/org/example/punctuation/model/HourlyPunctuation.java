package org.example.punctuation.model;

/**
 * Hourly punctuation following Tucker et al. 2003.
 * Semantics: "No more data for this sensor for this hour will arrive."
 */
public class HourlyPunctuation implements StreamElement {
    private String sid;      // Sensor ID
    private int hour;        // The hour for which data is complete
    private long timestamp;

    public HourlyPunctuation() {}

    public HourlyPunctuation(String sid, int hour, long timestamp) {
        this.sid = sid;
        this.hour = hour;
        this.timestamp = timestamp;
    }

    @Override
    public boolean isPunctuation() {
        return true;
    }

    @Override
    public String getKey() {
        return sid + "-" + hour;
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    public String getSid() {
        return sid;
    }

    public int getHour() {
        return hour;
    }

    public void setSid(String sid) {
        this.sid = sid;
    }

    public void setHour(int hour) {
        this.hour = hour;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "HourlyPunctuation{" +
                "sid='" + sid + '\'' +
                ", hour=" + hour +
                ", timestamp=" + timestamp +
                '}';
    }
}
