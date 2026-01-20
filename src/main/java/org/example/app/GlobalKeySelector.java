package org.example.app;

import org.apache.flink.api.java.functions.KeySelector;
import org.example.model.StreamElement;

/**
 * Routes all elements to a single global partition.
 * <p>
 * Used for PunctuationInjector to see all medallion transitions in order.
 */
public class GlobalKeySelector implements KeySelector<StreamElement, String> {

    @Override
    public String getKey(StreamElement element) {
        return "global";
    }
}
