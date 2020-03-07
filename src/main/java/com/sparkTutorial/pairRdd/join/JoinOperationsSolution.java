package com.sparkTutorial.pairRdd.join;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.util.Arrays;

public class JoinOperationsSolution {

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

        JavaPairRDD<String, Tuple2<Integer, String>> join = ages.join(addresses);
        System.out.println("Inner join ages with address");
        for (Tuple2<String, Tuple2<Integer, String>> eachEntry : join.collect()) {
            System.out.println(eachEntry);
        }

        JavaPairRDD<String, Tuple2<Integer, Optional<String>>> leftOuterJoin = ages.leftOuterJoin(addresses);
        System.out.println("\nLeft outer join");
        for (Tuple2<String, Tuple2<Integer, Optional<String>>> eachEntry : leftOuterJoin.collect()) {
            System.out.println(eachEntry);
        }

        JavaPairRDD<String, Tuple2<Optional<Integer>, String>> rightOuterJoin = ages.rightOuterJoin(addresses);

        System.out.println("\nRight outer join");
        for (Tuple2<String, Tuple2<Optional<Integer>, String>> eachEntry : rightOuterJoin.collect()) {
            System.out.println(eachEntry);
        }

        JavaPairRDD<String, Tuple2<Optional<Integer>, Optional<String>>> fullOuterJoin = ages.fullOuterJoin(addresses);

        System.out.println("\nFull outer join");
        for (Tuple2<String, Tuple2<Optional<Integer>, Optional<String>>> eachEntry : fullOuterJoin.collect()) {
            System.out.println(eachEntry);
        }



    }
}
