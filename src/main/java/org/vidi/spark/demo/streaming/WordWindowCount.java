package org.vidi.spark.demo.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.List;

/**
 * @author vidi
 * @date 2019-03-28
 */
public class WordWindowCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("JavaWordWindowCount")
                .setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

        // 日志格式
        // xxx xxx
        JavaReceiverInputDStream<String> searchLogsDStream = jssc.socketTextStream("localhost", 9999);

        JavaDStream<String> searchWordsDStream = searchLogsDStream.map(searchLogs -> searchLogs.split(" ")[1]);

        JavaPairDStream<String, Integer> searchWordsPairDStream = searchWordsDStream.mapToPair(searchLog -> new Tuple2<>(searchLog, 1));
        JavaPairDStream<String, Integer> searchWordCountsDStream = searchWordsPairDStream.reduceByKeyAndWindow(
                (Function2<Integer, Integer, Integer>) Integer::sum, Durations.seconds(60), Durations.seconds(10));

        JavaPairDStream<String, Integer> finalDStream = searchWordCountsDStream
                .transformToPair((Function<JavaPairRDD<String, Integer>, JavaPairRDD<String, Integer>>) searchWordCountsRDD -> {
                    JavaPairRDD<Integer, String> countSearchWordsRDD = searchWordCountsRDD.mapToPair(searchWordCount -> new Tuple2<>(searchWordCount._2, searchWordCount._1));
                    JavaPairRDD<Integer, String> sortedCountSearchWordsRDD = countSearchWordsRDD.sortByKey(false);
                    JavaPairRDD<String, Integer> sortedSearchWordCountsRDD = sortedCountSearchWordsRDD.mapToPair(sortedCountSearchWord -> new Tuple2<>(sortedCountSearchWord._2, sortedCountSearchWord._1));

                    // Take top 3.
                    List<Tuple2<String, Integer>> hotSearchWordCounts = sortedSearchWordCountsRDD.take(3);
                    // Executions about top 3.
                    return searchWordCountsRDD;
                });

        finalDStream.print();
        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }
}
