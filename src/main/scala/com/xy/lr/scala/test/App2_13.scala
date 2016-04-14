package com.xy.lr.scala.test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by xylr on 16-4-12.
  * com.xy.lr.scala.test
  */
object App2_13 {
  def main(args : Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("App")
    val sc = new SparkContext(conf)

    val rdd1 = sc.makeRDD(1 to 3, 1)
    val rdd2 = sc.makeRDD(2 to 4, 1)

    val unionRDD = rdd1.union(rdd2)//并集

    val intersectionRDD = rdd1.intersection(rdd2)//交集

    val subStractRDD = rdd1.subtract(rdd2)//差集
  }


}
