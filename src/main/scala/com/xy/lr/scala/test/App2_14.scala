package com.xy.lr.scala.test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by xylr on 16-4-12.
  * com.xy.lr.scala.test
  */
object App2_14 extends App {
  val conf = new SparkConf().setMaster("local[2]").setAppName("App")
  val sc = new SparkContext(conf)

  val rdd = sc.makeRDD(1 to 5, 2)

  val mapRDD = rdd.map(x => (x, x))

  val groupRDD = mapRDD.groupByKey(3)

  val mapPartitionsRDD = groupRDD.mapPartitions(iter => iter.filter(_._1 > 3))

  mapPartitionsRDD.collect()

}
