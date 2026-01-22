package org.example.model;

import java.io.Serializable;
import java.util.Map;

public class Tuple implements StreamElement {
    public Map<String, Object> schema;
    public long timestamp;

    public Tuple(Map<String, Object> schema, long timestamp) {
        this.schema = schema;
        this.timestamp = timestamp;
    }

    public Object getAttribute(String attribute) {
        return schema.get(attribute);
    }

    @Override
    public boolean isData() { return true; }

    @Override
    public boolean isPunctuation() { return false; }

    @Override
    public String toString() { return "Tuple(" + schema + ", " + timestamp + ")"; }
}
