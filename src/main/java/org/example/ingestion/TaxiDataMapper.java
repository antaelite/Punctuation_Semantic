package org.example.ingestion;

import org.apache.flink.api.common.functions.MapFunction;
import org.example.model.TaxiRide;

public class TaxiDataMapper implements MapFunction<String, TaxiRide> {

    @Override
    public TaxiRide map(String value) throws Exception {
        String[] values = value.split(",");
        if (values.length < 12 || value.startsWith("medallion")) {
            return null;
        }
        try {
            return new TaxiRide(values[0], values[1], values[2], values[5], values[6],
                    Double.parseDouble(values[9]), values[10], values[11], values[12], values[13]);
        } catch (Exception e) {
            return null;
        }
    }
}
