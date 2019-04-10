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
public class SortWordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("JavaSortWordCount")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("/Users/tanglei/workplace/about_hadoop/spark1-demo/data/shakespeare.txt");

        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")));

        JavaPairRDD<String, Integer> pairs = words.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey(Integer::sum);
        JavaPairRDD<Integer, String> countWords = wordCounts.mapToPair(wordCount -> new Tuple2<>(wordCount._2, wordCount._1));

        // 翻转 KV
        JavaPairRDD<String, Integer> result = countWords.sortByKey(false).mapToPair(tuple2 -> new Tuple2<>(tuple2._2, tuple2._1));

        result.foreach(tuple2 -> System.out.println(tuple2._1 + "    " + tuple2._2));
        sc.close();
    }
}
