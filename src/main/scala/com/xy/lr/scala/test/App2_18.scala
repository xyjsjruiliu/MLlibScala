package com.xy.lr.scala.test

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.HashSet

/**
  * Created by xylr on 16-4-13.
  * com.xy.lr.scala.test
  */
object App2_18 {
  def main(args : Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("App")
    val sc = new SparkContext(conf)

    val pairs = sc.parallelize(Array((1, 1), (1, 2), (1, 3), (1, 1), (2, 1)), 2)
    val bufs = pairs.mapValues(v => HashSet(v))

    val sums = bufs.foldByKey(new HashSet[Int])(_ ++= _)

    sums.collect()

    val reduceByKeyRDD = pairs.reduceByKey(_ + _)

    reduceByKeyRDD.collect()

    val groupByKeyRDD = pairs.groupByKey()

    groupByKeyRDD.collect()
  }


}
