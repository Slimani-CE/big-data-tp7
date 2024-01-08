package com.slimani.parte1.question1;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

// Une application qui affiche les projets encore de realisation
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

        // Lecture de données utilisant JDBC
        Dataset<Row> dataframe = spark
                .read()
                .format("jdbc")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("url", "jdbc:mysql://localhost:3306/db_imomaroc")
                .option("dbtable", "projets")
                .option("user", "root")
                .option("password", "")
                .load();

        // Affichage des projets encore de realisation
        dataframe.filter("date_fin > now()").show();
    }
}
