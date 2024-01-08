package com.slimani.parte1.question3;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.current_date;
import static org.apache.spark.sql.functions.datediff;

// Une application qui affiche pour chaque projet, les taches en retard avec la durée de retard
// Sachant que
// PROJETS(ID_PROJET, TITRE, DESCRIPTION, LIEU, DATE_DEBUT, DATE_FIN)
// TACHES(ID_TACHE, TITRE, DATE_DEBUT, DATE_FIN, TERMINE, ID_PROJET)
public class App {
    public static void main(String[] args) {
        // Connection SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("Application")
                .master("local[*]")
                .getOrCreate();

        // Lecture de données utilisant JDBC (PROJETS)
        Dataset<Row> projets = spark
                .read()
                .format("jdbc")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("url", "jdbc:mysql://localhost:3306/db_imomaroc")
                .option("query", "select * from projets")
                .option("user", "root")
                .option("password", "")
                .load();

        // Lecture de données utilisant JDBC (TACHES)
        Dataset<Row> taches = spark
                .read()
                .format("jdbc")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("url", "jdbc:mysql://localhost:3306/db_imomaroc")
                .option("query", "select * from taches")
                .option("user", "root")
                .option("password", "")
                .load();

        Dataset<Row> joined = projets.join(taches, projets.col("ID_PROJET").equalTo(taches.col("ID_PROJET")));

        // Filter the tasks that are not finished and are delayed
        Dataset<Row> delayedTasks = joined.filter(taches.col("TERMINE").equalTo(0)
                .and(taches.col("DATE_FIN").lt(current_date())));

        // Select the necessary fields
        Dataset<Row> result = delayedTasks.select(projets.col("ID_PROJET"), projets.col("TITRE").as("PROJET_TITRE"),
                taches.col("TITRE").as("TACHE_TITRE"), taches.col("DATE_DEBUT"), taches.col("DATE_FIN"),
                datediff(current_date(), taches.col("DATE_FIN")).as("RETARD"));

        // Display the result
        result.show();

        // Close the Spark session
        spark.close();
    }
}
