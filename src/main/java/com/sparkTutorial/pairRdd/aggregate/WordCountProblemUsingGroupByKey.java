package com.sparkTutorial.pairRdd.aggregate;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordCountProblemUsingGroupByKey {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> words = sc.textFile("in/word_count.text").flatMap(line-> Arrays.asList(line.split(" ")).iterator());
        /**
         *
         * 1) Convert words RDD to pair RDD and
         * 2) perform word count using groupByKey method
         *
         */

        JavaPairRDD<String, Iterable<Integer>> stringIterableJavaPairRDD = words.mapToPair(word -> new Tuple2<>(word, 1)).groupByKey();



    }
}
