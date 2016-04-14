package com.xy.lr.scala.test

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by xylr on 16-4-12.
  * com.xy.lr.scala.test
  */
object App2_12  {
  def main(args : Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("App")
    val sc = new SparkContext(conf)

    val a = ArrayBuffer

    val rdd = sc.makeRDD(1 to 10, 3)

    rdd.collect()

    val glomRDD = rdd.glom()

    glomRDD.collect()

    val splitRDD = rdd.randomSplit(Array(1.0, 3.0, 6.0))

    splitRDD(0).count()
    splitRDD(1).count()
    splitRDD(2).count()
  }

}
