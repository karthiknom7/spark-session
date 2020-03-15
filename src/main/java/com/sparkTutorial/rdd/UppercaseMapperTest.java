package com.sparkTutorial.rdd;

import com.holdenkarau.spark.testing.JavaRDDComparisons;
import com.holdenkarau.spark.testing.SharedJavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class UppercaseMapperTest extends SharedJavaSparkContext implements Serializable {


    private UppercaseMapper uppercaseMapper = new UppercaseMapper();

    @Test
    public void verifyMapTest() {
        List<String> lines = Arrays.asList("line one", "line two");
        JavaRDD<String> inputRdd = jsc().parallelize(lines);

        List<String> expectedWords = Arrays.asList("LINE", "ONE", "LINE", "TWO");
        JavaRDD<String> expectedWordsRdd = jsc().parallelize(expectedWords);

        JavaRDD<String> resultRdd = uppercaseMapper.mapToUpperCase(inputRdd);

        JavaRDDComparisons.assertRDDEquals(expectedWordsRdd, resultRdd);


    }

}