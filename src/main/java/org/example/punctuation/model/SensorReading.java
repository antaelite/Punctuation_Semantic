package org.example.punctuation.model;

import java.util.Objects;

/**
 * Temperature sensor reading following Tucker et al. 2003 schema.
 * Schema: <sid, hour, minute, curtmp>
 */
public class SensorReading implements StreamElement {
    private String sid;      // Sensor ID
    private int hour;        // Hour of the reading
    private int minute;      // Minute within the hour
    private double curtmp;   // Current temperature
    private long timestamp;

    public SensorReading() {}

    public SensorReading(String sid, int hour, int minute, double curtmp, long timestamp) {
        this.sid = sid;
        this.hour = hour;
        this.minute = minute;
        this.curtmp = curtmp;
        this.timestamp = timestamp;
    }

    @Override
    public boolean isPunctuation() {
        return false;
    }

    @Override
    public String getKey() {
        return sid + "-" + hour;  // Key by sensor and hour for aggregation
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

    public int getMinute() {
        return minute;
    }

    public double getCurtmp() {
        return curtmp;
    }

    public void setSid(String sid) {
        this.sid = sid;
    }

    public void setHour(int hour) {
        this.hour = hour;
    }

    public void setMinute(int minute) {
        this.minute = minute;
    }

    public void setCurtmp(double curtmp) {
        this.curtmp = curtmp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SensorReading that = (SensorReading) o;
        return hour == that.hour &&
                minute == that.minute &&
                Double.compare(that.curtmp, curtmp) == 0 &&
                Objects.equals(sid, that.sid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sid, hour, minute, curtmp);
    }

    @Override
    public String toString() {
        return "SensorReading{" +
                "sid='" + sid + '\'' +
                ", hour=" + hour +
                ", minute=" + minute +
                ", curtmp=" + curtmp +
                '}';
    }
}
