package com.xy.lr.scala.test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by xylr on 16-4-12.
  * com.xy.lr.scala.test
  */
object App2_15 {
  def main(args : Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("App")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(1 to 5, 2)
    val mapRDD = rdd.map(x => (x + 1.0))

    val zipRDD = rdd.zip(mapRDD)

    val rdd1 = sc.makeRDD(Array("1","2","3","4","5","6"), 2)

    val zipPartitionsRDD = rdd.zipPartitions(rdd1)((i : Iterator[Int], s: Iterator[String]) => {
      Iterator(i.toArray.size, s.toArray.size)
    })

    rdd.partitions(0)
  }

}
