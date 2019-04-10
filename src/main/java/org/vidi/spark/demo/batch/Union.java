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
public class Union {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("JavaUnion")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> department1StaffList = Arrays.asList("张三", "李四", "王二", "麻子");
        List<String> department2StaffList = Arrays.asList("赵六", "王五", "小明", "小倩");

        JavaRDD<String> department1StaffRDD = sc.parallelize(department1StaffList);
        JavaRDD<String> department2StaffRDD = sc.parallelize(department2StaffList);

        JavaRDD<String> departmentStaffRDD = department1StaffRDD.union(department2StaffRDD);

        departmentStaffRDD.collect().forEach(System.out::println);

        sc.close();
    }
}
