package org.example.punctuation.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.example.punctuation.model.HourlyPunctuation;
import org.example.punctuation.model.SensorReading;
import org.example.punctuation.model.StreamElement;

import java.util.Random;

/**
 * Intelligent Temperature Sensor Source following Tucker et al. 2003.
 * Emits sensor readings and punctuations at the end of each hour.
 * 
 * Simulates multiple sensors generating temperature readings continuously,
 * with punctuations signaling "no more data for hour X will arrive."
 */
public class TemperatureSensorSource implements SourceFunction<StreamElement> {
    private volatile boolean isRunning = true;
    private final Random random = new Random();
    private final String[] sensorIds;
    private final int durationHours;
    private final int readingsPerMinute;

    public TemperatureSensorSource(String[] sensorIds, int durationHours, int readingsPerMinute) {
        this.sensorIds = sensorIds;
        this.durationHours = durationHours;
        this.readingsPerMinute = readingsPerMinute;
    }

    @Override
    public void run(SourceContext<StreamElement> ctx) throws Exception {
        int currentHour = 0;
        int currentMinute = 0;

        while (isRunning && currentHour < durationHours) {
            // Generate readings for all sensors at this minute
            for (String sensorId : sensorIds) {
                // Generate multiple readings per minute (simulating high frequency)
                for (int i = 0; i < readingsPerMinute; i++) {
                    double temperature = 15.0 + random.nextDouble() * 20.0; // 15-35°C
                    long timestamp = System.currentTimeMillis();
                    
                    SensorReading reading = new SensorReading(
                        sensorId,
                        currentHour,
                        currentMinute,
                        temperature,
                        timestamp
                    );
                    
                    ctx.collect(reading);
                }
            }

            // Advance time
            currentMinute++;
            
            // End of hour: emit punctuations for all sensors
            if (currentMinute >= 60) {
                System.out.println("SOURCE: End of hour " + currentHour + " - emitting punctuations");
                
                for (String sensorId : sensorIds) {
                    HourlyPunctuation punctuation = new HourlyPunctuation(
                        sensorId,
                        currentHour,
                        System.currentTimeMillis()
                    );
                    ctx.collect(punctuation);
                    System.out.println("SOURCE: Punctuation emitted for sensor=" + sensorId + ", hour=" + currentHour);
                }
                
                currentMinute = 0;
                currentHour++;
            }

            // Sleep to simulate real-time (adjustable for demo speed)
            Thread.sleep(50); // Faster for demo purposes
        }
        
        System.out.println("SOURCE: Completed " + durationHours + " hours of simulation");
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
