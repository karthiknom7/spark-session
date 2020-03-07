package com.sparkTutorial.sparksql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;

public class ForecastSolution {

    private static String DIVISION = "division";
    private static String CATEGORY = "category";
    private static String YEAR = "year";
    private static String PERIOD = "period";
    private static String WEEK = "week";
    private static String BUCKET = "bucket";
    private static String VALUE = "value";

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession session = SparkSession.builder().appName("ForecastSolution").master("local[1]").getOrCreate();

        Dataset<Row> forecastData = session.read().option("header", "true").csv("in/forecast.csv");

        System.out.println("=== Print out schema ===");
        forecastData.printSchema();

        System.out.println("=== Print 20 records of responses table ===");
        forecastData.show(20);

        System.out.println("=== Print the division and category columns ===");
        forecastData.select(col(DIVISION),  col(CATEGORY)).show();

        System.out.println("=== Print records where category is 100, period is 13 and bucket is AD ===");
        forecastData.filter(col(CATEGORY).equalTo("100"))
                .filter(col(PERIOD).equalTo("13"))
                .filter(col(BUCKET).equalTo("AD")).show();

        System.out.println("=== Print the count of buckets ===");
        RelationalGroupedDataset relationalGroupedDataset = forecastData.groupBy(col(BUCKET));
        relationalGroupedDataset.count().show();

        System.out.println("=== Cast the year, period, week to integer and value to double  ===");

        Dataset<Row> castedForecastData = forecastData.withColumn(YEAR, col(YEAR).cast("integer"))
                .withColumn(PERIOD, col(PERIOD).cast("integer"))
                .withColumn(WEEK, col(WEEK).cast("integer"))
                .withColumn(VALUE, col(VALUE).cast("double"));

        System.out.println("=== Print out casted schema ===");
        castedForecastData.printSchema();


        System.out.println("=== Print categories total of each bucket === ");

        castedForecastData.select(col(CATEGORY), col(BUCKET), col(VALUE))
                .groupBy(col(CATEGORY), col(BUCKET)).sum(VALUE)
                .orderBy(col(CATEGORY), col(BUCKET)).show();

        System.out.println("=== Print the category which has total forecast less than other categories === ");



        session.stop();


    }
}
