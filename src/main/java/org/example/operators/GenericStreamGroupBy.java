package org.example.operators;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.example.core.*;
import org.example.model.*;


import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Instant;
import java.util.Map;

public class GenericStreamGroupBy extends PunctuatedIterator {

    private final SerializableKeyExtractor keyExtractor;
    private final AggregationStrategy strategy;
    private final int nbPunctuations;
    private MapState<String, Double> state;
    private ValueState<Long> lastClosedWindowEnd;
    private long startTime;
    private long firstOutputLatency = -1; // -1 tant qu'on n'a rien émis
    private boolean isFirstOutput = true;
    private PrintWriter csvWriter;

    public GenericStreamGroupBy(SerializableKeyExtractor keyExtractor, AggregationStrategy strategy,int nbPunctuations) {
        this.keyExtractor = keyExtractor;
        this.strategy = strategy;
        this.nbPunctuations = nbPunctuations;
    }         // L'opération (SUM, MAX, etc.)




    @Override
    public void open(Configuration parameters) {
        state = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("genericState", String.class, Double.class)
        );
        lastClosedWindowEnd = getRuntimeContext().getState(
                new ValueStateDescriptor<>("lastClosedWindow", Long.class)
        );
        this.startTime = System.currentTimeMillis();

        try {
            String csvFilename = "analysis/Performance.csv";
            File file = new File(csvFilename);
            boolean isNewFile = !file.exists();

            // Mode "true" pour append (ajouter à la suite sans écraser)
            csvWriter = new PrintWriter(new FileWriter(file, true));

            // On écrit l'en-tête seulement si le fichier est nouveau
            if (isNewFile) {
                csvWriter.println("latency,TimeLast,nbPunctuations");
                csvWriter.flush();
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize CSV writer", e);
        }

    }

    @Override
    public void step(TaxiRide ride, Context context, Collector<StreamItem> out) throws Exception {
        String key = keyExtractor.apply(ride);
        Double current = state.get(key);
        Long threshold = lastClosedWindowEnd.value();

        if (threshold != null && ride.getDropoffTimestamp() <= threshold) {
            System.out.println("LATE DATA DROPPED: " + ride.medallion + " at " + ride.dropoffDatetime);
            return;
        }
        if (current == null) current = 0.0;

        state.put(key, strategy.aggregate(current, ride));
    }

    @Override
    public void pass(Punctuation p, Context context, Collector<StreamItem> out) throws Exception {
        for (Map.Entry<String, Double> entry : state.entries()) {
            String start = Instant.ofEpochMilli(p.getStart()).toString();
            String end = Instant.ofEpochMilli(p.getEnd()).toString();
            if (isFirstOutput) {
                firstOutputLatency = System.currentTimeMillis() - startTime;
                isFirstOutput = false;
                // Note : On n'écrit PAS encore dans le CSV, on attend la fin
            }
            // On utilise le constructeur adapté
            out.collect(new GroupByResult(
                    entry.getKey(),
                    entry.getValue(),
                    start,
                    end
            ));
        }
    }
    @Override
    public void keep(Punctuation p, Context context) throws Exception {
        state.clear();
        lastClosedWindowEnd.update(p.getEnd());
    }

    @Override
    public void prop(Punctuation p, Context context, Collector<StreamItem> out) {
        out.collect(p);
    }

    @Override
    public void close() throws Exception {
        // C'est ICI qu'on a toutes les infos pour remplir la ligne
        if (csvWriter != null) {
            long timeLast = System.currentTimeMillis() - startTime; // Temps Total

            // Si aucune donnée n'est sortie (ex: dataset vide), latence reste -1 ou prend le temps total
            if (firstOutputLatency == -1) firstOutputLatency = timeLast;

            // Écriture de la ligne complète : latency, TimeLast, nbPunctuations
            csvWriter.println(firstOutputLatency + "," + timeLast + "," + nbPunctuations);
            csvWriter.flush();
            csvWriter.close();
        }
        super.close();
    }
}