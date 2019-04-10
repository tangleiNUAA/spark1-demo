package org.vidi.spark.demo.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * @author vidi
 * @date 2019-03-28
 */
public class JSONDataSource {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("JavaJsonDataSource")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        DataFrame employeeDF = sqlContext.read().format("json").load("/Users/tanglei/workplace/about_hadoop/spark1-demo/data/employee.json");
        employeeDF.registerTempTable("employee");

        DataFrame df = sqlContext.sql("select * from employee where salary > 18000");
        df.show();
        df.write().json("/Users/tanglei/workplace/about_hadoop/spark1-demo/data/result");
        sc.close();
    }
}
