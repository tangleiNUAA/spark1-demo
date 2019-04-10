package org.vidi.spark.demo.performance;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.SizeEstimator;

/**
 * @author vidi
 */
public class RDDState {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("Java RDD State")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = sc.textFile("/Users/tanglei/workplace/about_hadoop/spark1-demo/data/shakespeare.txt");
        // SizeEstimator 估计当前集合的大小
        long rddSize = SizeEstimator.estimate(rdd);
        long emptyRddSize = SizeEstimator.estimate(sc.emptyRDD());
        sc.close();
    }
}
