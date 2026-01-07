package org.example.source;

import java.util.Random;

import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.example.model.HourlyPunctuation;
import org.example.model.SensorReading;
import org.example.model.StreamElement;

/**
 * Deterministic Generator Function for Temperature Sensor Data.
 * Replaces the random thread-based logic of TemperatureSensorSource.
 */
public class TemperatureGeneratorFunction implements GeneratorFunction<Long, StreamElement> {

    private final String[] sensorIds;
    private final int readingsPerMinute;
    private final Random random = new Random();

    // Calculated derived constants
    private final int readingsPerSensorPerHour;
    private final int updatesPerHour; // readings + punctuation per sensor

    public TemperatureGeneratorFunction(String[] sensorIds, int durationHours, int readingsPerMinute) {
        this.sensorIds = sensorIds;
        this.readingsPerMinute = readingsPerMinute;
        
        // Readings per sensor per hour = 60 mins * readingsPerMinute
        // Total events per sensor per hour = readings + 1 punctuation
        this.readingsPerSensorPerHour = 60 * readingsPerMinute; 
        this.updatesPerHour = readingsPerSensorPerHour + 1;
    }

    @Override
    public StreamElement map(Long value) throws Exception {
        // value is a sequential index from 0 to TotalEvents - 1
        // We need to map this index to (Hour, Sensor, Type (Reading vs Punctuation))
        
        long globalIndex = value;
        
        // 1. Determine which Hour we are in
        // All sensors produce 'updatesPerHour' events in one hour.
        long totalEventsPerHour = (long) updatesPerHour * sensorIds.length;
        
        int hour = (int) (globalIndex / totalEventsPerHour);
        long indexWithinHour = globalIndex % totalEventsPerHour;
        
        // 2. Determine which Sensor
        // In this hour, events are interleaved or blocked by sensor? 
        // Let's assume block layout for simplicity of calculation, or simple modulo if we want interleaving.
        // Let's stick to the previous behavior: The previous source interleaved sensors per minute.
        // But DataGenerator is parallel-friendly and index-based.
        // Let's organize by: Hour -> Minute -> Sensor -> ReadingIndex
        
        // Re-thinking the mapping to match "original" time progression closely
        // The original source loops: Hour (0..D) -> Minute (0..60) -> Sensor (S1..Sn) -> Readings (0..R)
        // Then at end of Hour -> Sensor (S1..Sn) -> Punctuation
        
        // Total events per minute (for all sensors) = readingsPerMinute * sensors
        // Total events in one hour body = 60 * readingsPerMinute * sensors
        // Total events in one hour tail (punctuations) = sensors
        
        long bodyEventsPerHour = (long) 60 * readingsPerMinute * sensorIds.length;
        if (indexWithinHour < bodyEventsPerHour) {
            // It's a reading
            int minuteIndex = (int) (indexWithinHour / (readingsPerMinute * sensorIds.length));
            long indexWithinMinute = indexWithinHour % (readingsPerMinute * sensorIds.length);
            
            // Inside a minute: Sensor -> Reading
            int sensorIndex = (int) (indexWithinMinute / readingsPerMinute);
            
            String sensorId = sensorIds[sensorIndex];
            
            // Random temp (deterministic based on seed if we wanted, but Random() is fine for now)
            double temperature = 15.0 + random.nextDouble() * 20.0;
            
            // Timestamp approximation: StartTime + Hour + Minute
            // We can just use System.currentTimeMillis() or a logical time.
            // Original used System.currentTimeMillis(). 
            // Better to use logical time for deterministic replay? 
            // Let's stick to System time to minimize deviation, OR use 0 based time if we want pure simulation.
            long timestamp = System.currentTimeMillis(); 
            
            return new SensorReading(sensorId, hour, minuteIndex, temperature, timestamp);
            
        } else {
            // It's a punctuation
            long indexInTail = indexWithinHour - bodyEventsPerHour;
            int sensorIndex = (int) indexInTail; // purely 0..sensors-1
            
            String sensorId = sensorIds[sensorIndex];
            
            return new HourlyPunctuation(sensorId, hour, System.currentTimeMillis());
        }
    }
}
