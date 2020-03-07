package com.sparkTutorial.sparksql.join;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.max;

public class DepartmentForecastSolution {

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
        Dataset<Row> deptCategoryForecastData = forecastData.join(departmentCategoryMapping, forecastData.col(CATEGORY).equalTo(departmentCategoryMapping.col(CATEGORY)), "inner");
        deptCategoryForecastData.show(20);

        System.out.println("=== Print aggregated forecast numbers at department level");

        Dataset<Row> departmentForecastNumbers = deptCategoryForecastData.groupBy(col(DEPT), col(DIVISION), col(YEAR), col(PERIOD), col(WEEK), col(BUCKET)).sum(VALUE);
        departmentForecastNumbers.show();

        System.out.println("=== Print total forecast at dept, division and week level ");
        Dataset<Row> departmentTotalForecast = deptCategoryForecastData.groupBy(col(DEPT), col(DIVISION), col(YEAR), col(PERIOD), col(WEEK)).sum(VALUE);
        departmentTotalForecast.show();


        session.stop();


    }
}
