package com.xy.lr.scala.test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by xylr on 16-4-14.
  * com.xy.lr.scala.test
  */
object App2_25 extends App {
  val conf = new SparkConf().setMaster("local[2]").setAppName("App")
  val sc = new SparkContext(conf)

  val pairs = sc.makeRDD(Array(("a", 1), ("b", 2), ("a", 4), ("c", 5), ("a", 3)))
  val compareElement : ((String, Int), (String, Int)) => (String, Int) = (val1, val2) => {
    if (val1._2 >= val2._2)
      val1
    else
      val2
  }

  val foldResult = pairs.fold(("0", 0))(compareElement)
}
