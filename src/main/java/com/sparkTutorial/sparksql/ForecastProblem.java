package com.sparkTutorial.sparksql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

public class ForecastProblem {

    /*
    *
    *
    *
    *
         +--------+--------+----+------+----+------+-----------------+
        |division|category|year|period|week|bucket|            value|
        +--------+--------+----+------+----+------+-----------------+
        |     014|     100|2019|     1|   1|    AD|8242.790558470413|
        |     014|     100|2019|     1|   2|    AD|9961.014910505792|
        |     014|     100|2019|     1|   3|    AD|8693.483411188194|
        |     014|     100|2019|     1|   4|    AD|5733.514278847207|
        |     014|     100|2019|     2|   1|    AD|4045.630804069895|
        +--------+--------+----+------+----+------+-----------------+
    *
    * */

    private static String DIVISION = "division";
    private static String CATEGORY = "category";
    private static String YEAR = "year";
    private static String PERIOD = "period";
    private static String WEEK = "week";
    private static String BUCKET = "bucket";
    private static String VALUE = "value";

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession session = SparkSession.builder().appName("ForecastProblem").master("local[1]").getOrCreate();

        Dataset<Row> forecastData = session.read().option("header", "true").csv("in/forecast.csv");

        System.out.println("=== Print out schema ===");

        System.out.println("=== Print 20 records of responses table ===");

        System.out.println("=== Print the division and category columns ===");

        System.out.println("=== Print records where category is 100, period is 13 and bucket is AD ===");

        System.out.println("=== Print the count of buckets ===");


        System.out.println("=== Cast the year, period, week to integer and value to double  ===");


        System.out.println("=== Print out casted schema ===");


        System.out.println("=== Print categories total of each bucket === ");


        System.out.println("=== Print the category which has total forecast less than other categories === ");



        session.stop();


    }
}
