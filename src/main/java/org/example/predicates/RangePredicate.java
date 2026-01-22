package org.example.predicates;

import org.example.model.Tuple;

public class RangePredicate implements  PunctuationPredicate {
    private String attribute;
    private int min;
    private int max;

    public RangePredicate(String attribute, int min, int max) {
        this.attribute = attribute;
        this.min = min;
        this.max = max;
    }

    @Override
    public boolean matches(Tuple t) {
        Object val = t.getAttribute(attribute);
        if (val instanceof Integer) {
            int intVal = (Integer) val;
            return intVal >= min && intVal <= max;
        }
        return false;
    }

    @Override
    public boolean intersects(PunctuationPredicate p) {
        if (p instanceof RangePredicate) {
            RangePredicate rp = (RangePredicate) p;
            if (attribute.equals(rp.attribute)) {
                return min <= rp.max && rp.min <= max;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return attribute + ": [" + min + ", " + max + "]";
    }
}
