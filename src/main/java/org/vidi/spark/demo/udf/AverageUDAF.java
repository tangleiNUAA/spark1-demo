package org.vidi.spark.demo.udf;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * @author vidi
 */
public class AverageUDAF extends UserDefinedAggregateFunction {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Average UDAF Demo");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        sqlContext.udf().register("average_udf", new AverageUDAF());
        DataFrame employeeDF = sqlContext.read().format("json").load("/Users/tanglei/workplace/about_hadoop/spark1-demo/data/employee.json");
        employeeDF.registerTempTable("employee");
        employeeDF.show();

        DataFrame df = sqlContext.sql("select average_udf(salary) as average_salary FROM employee");
        df.show();
    }

    private StructType inputSchema;
    private StructType bufferSchema;

    public AverageUDAF() {
        List<StructField> inputFields = new ArrayList<>();
        inputFields.add(DataTypes.createStructField("inputColumn", DataTypes.LongType, true));
        inputSchema = DataTypes.createStructType(inputFields);

        List<StructField> bufferFields = new ArrayList<>();
        bufferFields.add(DataTypes.createStructField("sum", DataTypes.LongType, true));
        bufferFields.add(DataTypes.createStructField("count", DataTypes.LongType, true));
        bufferSchema = DataTypes.createStructType(bufferFields);
    }

    @Override
    public StructType inputSchema() {
        return inputSchema;
    }

    @Override
    public StructType bufferSchema() {
        return bufferSchema;
    }

    /**
     * Define result data type.
     *
     * @return result data type.
     */
    @Override
    public DataType dataType() {
        return DataTypes.DoubleType;
    }

    @Override
    public boolean deterministic() {
        return true;
    }

    /**
     * Initializes the given aggregation buffer. The buffer itself is a `Row` that in addition to
     * standard methods like retrieving a value at an index (e.g., get(), getBoolean()), provides
     * the opportunity to update its values. Note that arrays and maps inside the buffer are still
     * immutable.
     */
    @Override
    public void initialize(MutableAggregationBuffer buffer) {
        buffer.update(0, 0L);
        buffer.update(1, 0L);
    }

    /**
     * Updates the given aggregation buffer `buffer` with new input data from `input`
     */
    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {
        if (!input.isNullAt(0)) {
            long updateSum = buffer.getLong(0) + input.getLong(0);
            long mergedCount = buffer.getLong(1) + 1;
            buffer.update(0, updateSum);
            buffer.update(1, mergedCount);
        }
    }

    /**
     * Merges two aggregation buffers and stores the updated buffer values back to `buffer1`
     */
    @Override
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
        long updateSum = buffer1.getLong(0) + buffer2.getLong(0);
        long mergedCount = buffer1.getLong(1) + buffer2.getLong(1);
        buffer1.update(0, updateSum);
        buffer1.update(1, mergedCount);
    }

    @Override
    public Object evaluate(Row buffer) {
        return ((double) buffer.getLong(0)) / buffer.getLong(1);
    }
}
