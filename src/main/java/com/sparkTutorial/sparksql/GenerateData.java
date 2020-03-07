package com.sparkTutorial.sparksql;

import com.sparkTutorial.sparksql.dao.ForecastNumber;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class GenerateData {

    private static final String COMMA = ",";
    public static void main(String[] args) {

        // Create a Java Spark Context.
        //Logger.getLogger("org").setLevel(Level.ERROR);
       // SparkConf conf = new SparkConf().setAppName("uppercase2").setMaster("local[1]");

       // JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> divisions = Arrays.asList("014", "011", "015");
        List<String> categories = Arrays.asList("100", "150", "111", "101");
        List<String> buckets = Arrays.asList("AD", "NON_AD", "PPG", "WT");
        Integer year = 2019;
        List<Integer> periods = IntStream.rangeClosed(1, 13).boxed().collect(Collectors.toList());
        List<Integer> weeks = IntStream.rangeClosed(1, 4).boxed().collect(Collectors.toList());

        List<String> forecastNumbers = divisions.stream().flatMap(division -> {
            return categories.stream().flatMap(category -> {
                return buckets.stream().flatMap(bucket -> {
                    return periods.stream().flatMap(period -> {
                        return weeks.stream().map(week ->
                            division + COMMA +
                                    category + COMMA +
                                    year + COMMA +
                                    period + COMMA +
                                    week + COMMA +
                                    bucket + COMMA +
                                    randomValue());
                    });
                });
            });
        }).collect(Collectors.toList());
        System.out.println(forecastNumbers.size());
        for (String forecastNumber : forecastNumbers) {
            System.out.println(forecastNumber);
        }

//        JavaRDD<String> forecastNumberJavaRDD = sc.parallelize(forecastNumbers);
//        forecastNumberJavaRDD.saveAsTextFile("out/forecastnumbers.txt");


    }

    private static double randomValue(){
        Random r = new Random();
        double min = 1000;
        double max = 10000;
        return min + (max - min) * r.nextDouble();
    }
}
