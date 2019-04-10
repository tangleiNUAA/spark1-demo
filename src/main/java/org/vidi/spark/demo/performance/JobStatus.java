package org.vidi.spark.demo.performance;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkJobInfo;
import org.apache.spark.SparkStageInfo;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author vidi
 */
public class JobStatus {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("JobInfo")
                .setMaster("Java Job Info.");
        JavaSparkContext sc = new JavaSparkContext(conf);

        int[] jobIds = sc.statusTracker().getActiveJobIds();
        int currentJobId = jobIds[jobIds.length - 1];
        for (int id : jobIds) {
            SparkJobInfo jobInfo = sc.statusTracker().getJobInfo(id);
            SparkStageInfo stageInfo = sc.statusTracker().getStageInfo(id);
            // Process these state info.
        }

        sc.close();
    }
}
