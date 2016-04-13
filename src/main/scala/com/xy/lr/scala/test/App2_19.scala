package com.xy.lr.scala.test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by xylr on 16-4-13.
  * com.xy.lr.scala.test
  */
object App2_19 extends App {
  val conf = new SparkConf().setMaster("local[2]").setAppName("App")
  val sc = new SparkContext(conf)

  val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1)), 1)
  val rdd2 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w')), 1)

  val cogroupRDD = rdd1.cogroup(rdd2)

  cogroupRDD.collect()

  val joinRDD = rdd1.join(rdd2)

  joinRDD.collect()


}
