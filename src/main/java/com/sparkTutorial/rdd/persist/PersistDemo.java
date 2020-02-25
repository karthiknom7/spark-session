package com.sparkTutorial.rdd.persist;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import java.util.Arrays;
import java.util.List;

public class PersistDemo {

    public static void main(String[] args) throws Exception {
        // Create a Java Spark Context.
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("persist").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<String> wordsList = Arrays.asList( "Trade", "People", "Steamboat");
        JavaRDD<String> words = sc.parallelize(wordsList);

        JavaRDD<String> lowerCase = words.map(word -> {
            System.out.println("First mapping  : " + word);
            return word.toLowerCase();
        });

        JavaRDD<String> upperCase = lowerCase.map(word -> {
            System.out.println("Second mapping : " + word);
            return word.toUpperCase();
        });

        //upperCase.persist(StorageLevel.MEMORY_ONLY());
        
        System.out.println("Collect action starting----------------------------------");
        List<String> collect = upperCase.collect();
        System.out.println("Collect action ended----------------------------------");
        System.out.println();
        System.out.println();
        System.out.println("Count action starting------------------------------------");
        long count = upperCase.count();
        System.out.println("Count action ended------------------------------------");


    }
}
