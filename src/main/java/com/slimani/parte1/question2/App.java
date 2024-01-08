package com.slimani.parte1.question2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

// Une pour chaque projet, le nombre de taches dont la durée dépasse un mois
// Le format de sortie est le suivant:
// ID_PROJET | TITRE | NOMBRE
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
                .option("query", "select id_projet, titre from projets")
                .option("user", "root")
                .option("password", "")
                .load();

        // Lecture de données utilisant JDBC (TACHES)
        Dataset<Row> taches = spark
                .read()
                .format("jdbc")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("url", "jdbc:mysql://localhost:3306/db_imomaroc")
                .option("query", "select id_projet, date_debut, date_fin from taches")
                .option("user", "root")
                .option("password", "")
                .load();

        // Une pour chaque projet, le nombre de taches dont la durée dépasse un mois
        // Le format de sortie est le suivant:
        // ID_PROJET | TITRE | NOMBRE
        projets.join(taches, "id_projet")
                .filter("datediff(date_fin, date_debut) > 30")
                .groupBy("id_projet", "titre")
                .count()
                .show();
        // Fermeture de la session Spark
        spark.close();
    }
}
