package com.xy.lr.scala.test

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD._

/**
  * Created by xylr on 16-4-12.
  * com.xy.lr.scala.test
  */
object App2_11 extends App {
  val conf = new SparkConf().setMaster("local[2]").setAppName("App")
  val sc = new SparkContext(conf)

  val rdd = sc.makeRDD(1 to 10, 100)

  val repartitionRDD = rdd.repartition(4)

//  repartitionRDD.collectPartitions()
}
