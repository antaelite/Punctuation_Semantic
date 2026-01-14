package org.example.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.example.model.HourlyPunctuation;
import org.example.model.StreamElement;
import org.example.model.TaxiRide;

import java.io.BufferedReader;
import java.io.FileReader;

/**
 * Lit le fichier CSV des Taxis et injecte des Ponctuations Sémantiques.
 * Remplace TemperatureGeneratorFunction.
 */
public class TaxiFileSource implements SourceFunction<StreamElement> {

    private final String filePath;
    private volatile boolean isRunning = true;

    public TaxiFileSource(String filePath) {
        this.filePath = filePath;
    }

    @Override
    public void run(SourceContext<StreamElement> ctx) throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader(filePath));
        String line = reader.readLine(); // On saute le header

        TaxiRide lastRide = null;

        while (isRunning && (line = reader.readLine()) != null) {
            TaxiRide currentRide;
            try {
                currentRide = new TaxiRide(line);
            } catch (Exception e) {
                continue; // Ignorer lignes cassées
            }

            // --- LOGIQUE DE PONCTUATION (Cœur du papier) ---
            // Si l'heure a changé par rapport à la ligne précédente
            if (lastRide != null && currentRide.getHour() != lastRide.getHour()) {

                int finishedHour = lastRide.getHour();
                long timestamp = lastRide.getTimestamp();

                // On émet la ponctuation pour dire "Fini pour l'heure X"
                // On utilise la même clé "NYC-TAXI" que dans TaxiRide pour que ça matche
                System.out.println(">>> SOURCE: Emission ponctuation pour fin heure " + finishedHour);

                ctx.collect(new HourlyPunctuation("NYC-TAXI", finishedHour, timestamp));

                Thread.sleep(500);
            }

            // Émettre la donnée
            ctx.collect(currentRide);
            lastRide = currentRide;

            // Simuler un léger délai de traitement (optionnel)
            // Thread.sleep(1);
        }
        reader.close();
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
