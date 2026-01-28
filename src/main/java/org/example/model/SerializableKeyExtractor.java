package org.example.model;

import java.io.Serializable;
import java.util.function.Function;

/**
 * Interface pour l'extraction de clé de groupement.
 */
@FunctionalInterface
public interface SerializableKeyExtractor extends Function<TaxiRide, String>, Serializable {}
