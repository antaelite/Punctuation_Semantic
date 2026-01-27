package org.example.core;

import org.example.model.TaxiRide;

import java.io.Serializable;

@FunctionalInterface
public interface AggregationStrategy extends Serializable {
    double aggregate(Double current, TaxiRide ride);
}