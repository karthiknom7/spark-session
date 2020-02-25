package com.sparkTutorial.rdd;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class UppercaseWord2 {

    public static void main(String[] args) throws Exception {
        // Create a Java Spark Context.
        SparkConf conf = new SparkConf().setAppName("uppercase").setMaster("local[2]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("in/uppercase.text");
        /*
            1) Map lines to words
            2) Convert each word to upper case
            3) Count the number of words starts with 'A'
         */


    }
}
