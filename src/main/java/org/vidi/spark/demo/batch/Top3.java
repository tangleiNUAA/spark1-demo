package org.vidi.spark.demo.batch;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author vidi
 * @date 2019-03-28
 */
public class Top3 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("JavaTop3")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("/Users/tanglei/workplace/about_hadoop/spark1-demo/data/shakespeare.txt");

    }
}
