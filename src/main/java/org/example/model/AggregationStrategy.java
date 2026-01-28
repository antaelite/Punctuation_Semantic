package org.example.model;

import java.io.Serializable;

@FunctionalInterface
public interface AggregationStrategy extends Serializable {
    double aggregate(Double current, TaxiRide ride);
}