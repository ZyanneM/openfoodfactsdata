package com.openfoodfacts;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class Main {
    public static void main(String[] args) {

        Logger.getRootLogger().setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("OpenFoodFactsData")
                .config("spark.master", "local")
                .getOrCreate();

        StructType schema = new StructType()
                .add("product_name", DataTypes.StringType)
                .add("brands", DataTypes.StringType)
                .add("countries", DataTypes.StringType)
                .add("ingredients_text", DataTypes.StringType)
                .add("energy_100g", DataTypes.StringType)
                .add("sugars_100g", DataTypes.StringType)
                .add("fat_100g", DataTypes.StringType)
                .add("labels", DataTypes.StringType)
                .add("packaging", DataTypes.StringType);

        String csvFile = "src/main/java/com/openfoodfacts/en.openfoodfacts.org.products.csv";

        Dataset<Row> data = null;
        try {
            data = spark.read()
                    .option("header", "true")
                    .option("delimiter", "\t")
                    .schema(schema)
                    .csv(csvFile);
            if (data.isEmpty()) {
                System.out.println("Le fichier est vide ou mal formaté.");
            } else {
                System.out.println("Données chargées avec succès.");
                data.show(10);
                data.printSchema();
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Erreur lors du chargement du fichier CSV.");
        }

        spark.stop();
    }
}
