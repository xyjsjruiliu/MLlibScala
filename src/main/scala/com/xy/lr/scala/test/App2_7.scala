package com.xy.lr.scala.test

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.metrics.source.Source
import org.apache.spark.util.TaskCompletionListener
import org.apache.spark.{SparkConf, SparkContext, TaskContext}

/**
  * Created by xylr on 16-4-12.
  * com.xy.lr.scala.test
  */
object App2_7 {
  def main(args : Array[String]): Unit ={
    val conf = new SparkConf().setMaster("local[2]").setAppName("app")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(1 to 10, 2)
    val map_rdd = rdd.map(a => a+ 1)

    val filter_rdd = map_rdd.filter(a => (a > 3))



    println(filter_rdd.count())
  }

}
