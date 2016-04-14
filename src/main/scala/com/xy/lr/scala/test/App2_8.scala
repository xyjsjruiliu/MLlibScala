package com.xy.lr.scala.test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by xylr on 16-4-14.
  * com.xy.lr.scala.test
  */
object App2_8 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("App")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(1 to 10, 2).map(x => (x, x))

    rdd.partitioner
  }
}
