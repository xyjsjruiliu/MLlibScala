package com.xy.lr.scala.test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by xylr on 16-4-12.
  * com.xy.lr.scala.test
  */
object App2_10 extends App {
  val conf = new SparkConf().setMaster("local[2]").setAppName("App")
  val sc = new SparkContext(conf)

  val rdd = sc.makeRDD(1 to 5, 1)
  val mapRDD = rdd.map(x => x.toFloat)

  mapRDD.collect()

  val flatMapRDD = rdd.flatMap(x => (1 to x))

  flatMapRDD.collect()

  val distinctRDD = flatMapRDD.distinct()

  distinctRDD.collect()
}
