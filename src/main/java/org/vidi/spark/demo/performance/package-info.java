/**
 * 为 Spark 运行的性能需要做的一些工作。包括:
 * <p>
 * 在编写用于生产环境的计算前, 通过加载部分数据观察 RDD 状态预估 Application 运行时的状态。
 * <p>
 * 根据观察合理配置 Application.
 *
 * @author vidi
 */
package org.vidi.spark.demo.performance;