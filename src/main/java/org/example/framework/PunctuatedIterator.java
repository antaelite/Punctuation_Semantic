package org.example.framework;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.model.Punctuation;
import org.example.model.StreamItem;
import org.example.model.TaxiRide;

public abstract class PunctuatedIterator extends KeyedProcessFunction<String, StreamItem, StreamItem> {
    @Override
    public void processElement(StreamItem item, Context context, Collector<StreamItem> out) throws Exception{
        if (!item.isPunctuation()) {
            // Process tuple
            step((TaxiRide) item, context, out);
        } else {
            Punctuation p = (Punctuation) item;
            pass(p, context, out);
            prop(p, context, out);
            keep(p, context);
        }
    }

    public abstract void step(TaxiRide ride, Context context, Collector<StreamItem> out) throws Exception;
    public abstract void pass(Punctuation p, Context context, Collector<StreamItem> out) throws Exception;
    public abstract void prop(Punctuation p, Context context, Collector<StreamItem> out) throws Exception;
    public abstract void keep(Punctuation p, Context context) throws Exception;
}
