package com.openfoodfacts;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class Main {
    public static void main(String[] args) {

        Logger.getRootLogger().setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("OpenFoodFactsData")
                .config("spark.master", "local")
                .getOrCreate();

        String csvFile = "src/main/java/com/openfoodfacts/en.openfoodfacts.org.products.csv";

        // Charger uniquement un sous-ensemble de lignes pour optimiser la performance (ici, on charge les 100 premières lignes)
        Dataset<Row> data = null;

        try {
            data = spark.read()
                    .option("header", "true") // Première ligne comme en-tête
                    .option("inferSchema", "true")  // Laisser Spark inférer les types
                    .option("delimiter", "\t") // Délimiteur tabulation
                    .csv(csvFile)
                    .select("product_name", "brands", "countries", "ingredients_text", "energy_100g", "sugars_100g", "fat_100g", "labels", "packaging") // Sélectionner uniquement les colonnes nécessaires
                    .limit(11);  // Limite les 100 premières lignes pour optimiser le chargement

            // Vérifier si les données ont été chargées correctement
            if (data.isEmpty()) {
                System.err.println("Erreur : Le fichier est vide ou mal formaté.");
            } else {
                System.out.println("Données chargées avec succès !");

                // Appliquer les transformations pour s'assurer que les colonnes numériques soient bien de type Double
                Dataset<Row> transformedData = data
                        .withColumn("energy_100g", functions.when(functions.col("energy_100g").rlike("^[0-9]*\\.?[0-9]+$"),
                                        functions.col("energy_100g").cast("double"))
                                .otherwise(functions.lit(null)))  // Si ce n'est pas une chaîne valide, on met null
                        .withColumn("sugars_100g", functions.when(functions.col("sugars_100g").rlike("^[0-9]*\\.?[0-9]+$"),
                                        functions.col("sugars_100g").cast("double"))
                                .otherwise(functions.lit(null)))  // Si ce n'est pas une chaîne valide, on met null
                        .withColumn("fat_100g", functions.when(functions.col("fat_100g").rlike("^[0-9]*\\.?[0-9]+$"),
                                        functions.col("fat_100g").cast("double"))
                                .otherwise(functions.lit(null)));  // Si ce n'est pas une chaîne valide, on met null

                // Afficher les données transformées
                transformedData.show(10);

                // Vérification du schéma pour s'assurer que les types sont corrects
                transformedData.printSchema();
            }
        } catch (Exception e) {
            System.err.println("Erreur lors du chargement du fichier CSV : " + e.getMessage());
        } finally {
            spark.stop();
        }
    }
}
