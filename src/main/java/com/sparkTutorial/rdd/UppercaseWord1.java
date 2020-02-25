package com.sparkTutorial.rdd;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import java.util.Arrays;
import java.util.List;

public class UppercaseWord1 {

    public static void main(String[] args) throws Exception {
        // Create a Java Spark Context.
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("uppercase").setMaster("local[2]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> wordsList = Arrays.asList("advancements", "attacks", "trade", "American", "war", "Obama", "people", "steamboat");

        JavaRDD<String> words = sc.parallelize(wordsList);
        /*
            1) Convert each word to upper case
            2) Count the number of words starts with 'A'
         */
        JavaRDD<String> first_map = words.map(word -> {
            System.out.println("First map word : " + word);
            return word;
        });

        JavaRDD<String> secondMap = first_map.map(word -> {
            System.out.println("second map : " + word);
            return word.toUpperCase();
        });

        secondMap.persist(StorageLevel.MEMORY_ONLY());

        List<String> collect = secondMap.collect();
        System.out.println("Collect action----------------------------------");
        System.out.println();
        long count = secondMap.count();
        System.out.println("count action------------------------------------");


    }
}
