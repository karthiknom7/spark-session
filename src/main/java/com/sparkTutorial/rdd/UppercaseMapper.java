package com.sparkTutorial.rdd;

import org.apache.spark.api.java.JavaRDD;

import java.util.Arrays;

public class UppercaseMapper {

    public JavaRDD<String> mapToUpperCase(JavaRDD<String> inputRdd){
        JavaRDD<String> words = inputRdd.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        return words.map(String::toUpperCase);
    }
}
