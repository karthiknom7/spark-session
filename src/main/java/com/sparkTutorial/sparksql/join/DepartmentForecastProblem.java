package com.sparkTutorial.sparksql.join;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

public class DepartmentForecastProblem {

    /*
    *
    *   Forecast table
    *
        +--------+--------+----+------+----+------+-----------------+
        |division|category|year|period|week|bucket|            value|
        +--------+--------+----+------+----+------+-----------------+
        |     014|     100|2019|     1|   1|    AD|8242.790558470413|
        |     014|     100|2019|     1|   2|    AD|9961.014910505792|
        |     014|     100|2019|     1|   3|    AD|8693.483411188194|
        |     014|     100|2019|     1|   4|    AD|5733.514278847207|
        +--------+--------+----+------+----+------+-----------------+


        Dept and category mapping

        +----+--------+
        |dept|category|
        +----+--------+
        |  01|     100|
        |  01|     150|
        |  02|     111|
        |  02|     101|
        +----+--------+
    *
    * */


    private static String DIVISION = "division";
    private static String CATEGORY = "category";
    private static String YEAR = "year";
    private static String PERIOD = "period";
    private static String WEEK = "week";
    private static String BUCKET = "bucket";
    private static String VALUE = "value";
    private static String DEPT = "dept";

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession session = SparkSession.builder().appName("DepartmentForecastSolution").master("local[1]").getOrCreate();

        Dataset<Row> forecastData = session.read().option("header", "true").csv("in/forecast.csv")
                .withColumn(YEAR, col(YEAR).cast("integer")).withColumn(PERIOD, col(PERIOD).cast("integer"))
                .withColumn(WEEK, col(WEEK).cast("integer")).withColumn(VALUE, col(VALUE).cast("double"));

        Dataset<Row> departmentCategoryMapping = session.read().
                option("header", "true").csv("in/dept_category_mapping.csv");


        System.out.println("=== Join forecast data with dept-category mapping and print 20 records ===");

        System.out.println("=== Print aggregated forecast numbers at department level");

        System.out.println("=== Print total forecast at dept, division and week level ");



        session.stop();


    }
}
