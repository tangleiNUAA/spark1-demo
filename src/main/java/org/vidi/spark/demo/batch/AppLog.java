package org.vidi.spark.demo.batch;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * @author vidi
 * @date 2019-03-28
 */
public class AppLog {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("JavaApplicationLog")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> accessLogRDD = sc.textFile("/Users/tanglei/workplace/about_hadoop/spark1-demo/data/access.log");
        JavaPairRDD<String, AccessLogInfo> accessLogInfoJavaPairRDD = accessLogRDD.mapToPair(accessLog -> {
            String[] accessLogSplit = accessLog.split("\t");
            AccessLogInfo accessLogInfo = new AccessLogInfo(Long.parseLong(accessLogSplit[0]), Long.parseLong(accessLogSplit[2]), Long.parseLong(accessLogSplit[3]));
            return new Tuple2<>(accessLogSplit[1], accessLogInfo);
        });

        // 获取每个 Device 的上下行总量及最早访问时间戳
        JavaPairRDD<String, AccessLogInfo> aggrAccessLogPairRdd = accessLogInfoJavaPairRDD.reduceByKey((accessLogInfo1, accessLogInfo2) -> {
            long timestamp = accessLogInfo1.getTimestamp() < accessLogInfo2.getTimestamp() ?
                    accessLogInfo1.getTimestamp() : accessLogInfo2.getTimestamp();
            long upTraffic = accessLogInfo1.getUpTraffic() + accessLogInfo2.getUpTraffic();
            long downTraffic = accessLogInfo1.getDownTraffic() + accessLogInfo2.getDownTraffic();

            AccessLogInfo accessLogInfo = new AccessLogInfo();
            accessLogInfo.setTimestamp(timestamp);
            accessLogInfo.setUpTraffic(upTraffic);
            accessLogInfo.setDownTraffic(downTraffic);
            return accessLogInfo;
        });

        // 将按deviceID聚合RDD的key映射为二次排序key，value映射为deviceID
        JavaPairRDD<AccessLogSortKey, String> accessLogSortRDD = aggrAccessLogPairRdd.mapToPair(aggrAccessLogPair -> {
            String device = aggrAccessLogPair._1;
            AccessLogInfo accessLogInfo = aggrAccessLogPair._2;
            return new Tuple2<>(new AccessLogSortKey(accessLogInfo.getUpTraffic(), accessLogInfo.getDownTraffic(), accessLogInfo.getTimestamp()), device);
        });

        // 执行二次排序操作，按照上行流量、下行流量以及时间戳进行倒序排序
        JavaPairRDD<AccessLogSortKey, String> sortedAccessLogRDD = accessLogSortRDD.sortByKey(false);
        sortedAccessLogRDD.take(10).forEach(elememt -> System.out.println(elememt._2 + "    " + elememt._1));

        sc.close();
    }
}
