package org.example.core;
import org.example.core.StreamItem;

//
public class GroupByResult extends StreamItem {
    public String groupKey;      // Le médaillon, le nombre de passagers, etc.
    public double resultValue;   // Le résultat du calcul (somme, max, count...)
    public String windowStart;
    public String windowEnd;

    public GroupByResult(String groupKey, double resultValue, String windowStart, String windowEnd) {
        this.groupKey = groupKey;
        this.resultValue = resultValue;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
    }

    @Override
    public boolean isPunctuation() {
        return false;
    }

    @Override
    public String toString() {
        return String.format("Group: %s | Value: %.2f | Window: [%s - %s]",
                groupKey, resultValue, windowStart, windowEnd);
    }
}