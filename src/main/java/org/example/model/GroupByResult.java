package org.example.model;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.example.core.StreamItem;

@Value
@EqualsAndHashCode(callSuper = false)
public class GroupByResult extends StreamItem {
    public String groupKey;      // Le médaillon, le nombre de passagers, etc.
    public double resultValue;   // Le résultat du calcul (somme, max, count...)
    public String windowStart;
    public String windowEnd;

    @Override
    public boolean isPunctuation() {
        return false;
    }

//    @Override
//    public String toString() {
//        return String.format("Group: %s | Value: %.2f | Window: [%s - %s]",
//                groupKey, resultValue, windowStart, windowEnd);
//    }
}