package com.xy.lr.scala.test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by xylr on 16-4-12.
  * com.xy.lr.scala.test
  */
object App2_16  {
  def main(args : Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("App")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(1 to 6, 2)
    val zipWithIndex = rdd.zipWithIndex()

    zipWithIndex.collect()

    val zipWithUniqueId = rdd.zipWithUniqueId()

    zipWithUniqueId.collect()
  }

}
