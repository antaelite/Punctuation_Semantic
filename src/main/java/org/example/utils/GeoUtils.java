package org.example.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.awt.geom.Path2D;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GeoUtils {

    // Structure interne pour garder la forme de chaque quartier
    private static class BoroughShape {
        String name;
        Path2D shape;
        public BoroughShape(String name, Path2D shape) {
            this.name = name;
            this.shape = shape;
        }
    }

    private static List<BoroughShape> boroughs = new ArrayList<>();
    private static boolean isLoaded = false;

    // A appeler dans le open() de l'opérateur
    public static void loadBoroughs(String filePath) {
        if (isLoaded) return;
        try {
            ObjectMapper mapper = new ObjectMapper();
            // Lit le fichier GeoJSON
            JsonNode root = mapper.readTree(new File(filePath));
            JsonNode features = root.path("features");

            for (JsonNode feature : features) {
                // Récupère le nom (vérifie si c'est "borough" ou "BoroName" dans ton json)
                String name = feature.path("properties").path("borough").asText();
                if (name == null || name.isEmpty()) name = feature.path("properties").path("BoroName").asText();

                // Construit le polygone
                Path2D path = new Path2D.Double();
                JsonNode coords = feature.path("geometry").path("coordinates");
                String type = feature.path("geometry").path("type").asText();

                if ("Polygon".equals(type)) {
                    buildPolygon(path, coords);
                } else if ("MultiPolygon".equals(type)) {
                    for (JsonNode poly : coords) buildPolygon(path, poly);
                }
                boroughs.add(new BoroughShape(name, path));
            }
            System.out.println(">>> GeoUtils : " + boroughs.size() + " quartiers chargés.");
            isLoaded = true;
        } catch (IOException e) {
            System.err.println("ERREUR GeoUtils: " + e.getMessage());
        }
    }

    private static void buildPolygon(Path2D path, JsonNode coords) {
        JsonNode ring = coords.get(0); // On prend l'anneau extérieur
        if (ring != null) {
            boolean first = true;
            for (JsonNode point : ring) {
                double lon = point.get(0).asDouble();
                double lat = point.get(1).asDouble();
                if (first) { path.moveTo(lon, lat); first = false; }
                else { path.lineTo(lon, lat); }
            }
            path.closePath();
        }
    }

    public static String getBorough(double lon, double lat) {
        if (!isLoaded) return "NotLoaded";
        for (BoroughShape b : boroughs) {
            if (b.shape.contains(lon, lat)) return b.name;
        }
        return "Unknown";
    }
}