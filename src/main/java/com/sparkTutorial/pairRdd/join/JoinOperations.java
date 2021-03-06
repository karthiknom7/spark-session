package com.sparkTutorial.pairRdd.join;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.util.Arrays;

public class JoinOperations {

    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("JoinOperations").setMaster("local[1]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<String, Integer> ages = sc.parallelizePairs(Arrays.asList(new Tuple2<>("Tom", 29),
                                                                              new Tuple2<>("John", 22)));

        JavaPairRDD<String, String> addresses = sc.parallelizePairs(Arrays.asList(new Tuple2<>("James", "USA"),
                                                                                  new Tuple2<>("John", "UK")));

        /**
         * Perform inner join, left join, right join and outer join on above ages and addresses pair RDDs
         *
         */



    }
}
