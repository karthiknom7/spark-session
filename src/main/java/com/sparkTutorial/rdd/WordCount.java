package com.sparkTutorial.rdd;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.Map;

public class WordCount {

    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("in/word_count.text");
        JavaRDD<String> words = lines.flatMap(line -> {
            String[] strings = line.split(" ");
            return Arrays.asList(strings).iterator();
        });
        Map<String, Long> stringLongMap = words.countByValue();
        for (Map.Entry<String, Long> stringLongEntry : stringLongMap.entrySet()) {
            System.out.println(stringLongEntry.getKey() +"->"+ stringLongEntry.getValue());
        }


    }
}
