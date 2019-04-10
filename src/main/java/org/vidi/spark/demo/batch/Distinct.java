package org.vidi.spark.demo.batch;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * @author vidi
 * @date 2019-03-28
 */
public class Distinct {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("JavaDistinct")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> accessLogs = Arrays.asList(
                "user1 2016-01-01 23:58:42",
                "user1 2016-01-01 23:58:42",
                "user1 2016-01-01 23:58:43",
                "user1 2016-01-01 23:58:44",
                "user1 2016-01-01 23:58:44",
                "user1 2016-01-01 23:58:44",
                "user1 2016-01-01 23:58:44",
                "user1 2016-01-01 23:58:44",
                "user2 2016-01-01 12:58:42",
                "user2 2016-01-01 12:58:46",
                "user3 2016-01-01 12:58:42",
                "user3 2016-01-01 12:58:42",
                "user3 2016-01-01 12:58:42",
                "user4 2016-01-01 12:58:42",
                "user5 2016-01-01 12:58:42",
                "user5 2016-01-01 12:58:42",
                "user5 2016-01-01 12:58:42",
                "user6 2016-01-01 12:58:42",
                "user6 2016-01-01 12:58:42",
                "user6 2016-01-01 12:58:45");
        JavaRDD<String> accessLogRDD = sc.parallelize(accessLogs);

        JavaRDD<String> userIdsRdd = accessLogRDD.map(accessLog -> accessLog.split(" ")[0]);

        JavaRDD<String> distinctUserIdsRdd = userIdsRdd.distinct();
        int userCount = distinctUserIdsRdd.collect().size();
        System.out.println("userCount = " + userCount);
        sc.close();
    }
}
