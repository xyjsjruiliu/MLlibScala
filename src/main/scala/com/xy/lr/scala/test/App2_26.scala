package com.xy.lr.scala.test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by xylr on 16-4-14.
  * com.xy.lr.scala.test
  */
object App2_26 {
  def main(args : Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("App")
    val sc = new SparkContext(conf)

    val pairs = sc.makeRDD(Array(("a", 1), ("b", 2), ("a", 4), ("c", 5), ("a", 3)), 1)

    pairs.lookup("a")
  }

}
