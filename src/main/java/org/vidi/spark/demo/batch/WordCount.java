package org.vidi.spark.demo.batch;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author vidi
 * @date 2019-03-28
 */
public class WordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("JavaWordCount")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("/Users/tanglei/workplace/about_hadoop/spark1-demo/data/shakespeare.txt");

        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.replace(",", "").replace(";", "").replace(".", "").replace("'", "").split(" ")));
        JavaPairRDD<String, Integer> pairs = words.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey(Integer::sum);
        wordCounts.saveAsTextFile("/Users/tanglei/workplace/about_hadoop/spark1-demo/data/word_frequency.txt");
        sc.close();
    }
}
