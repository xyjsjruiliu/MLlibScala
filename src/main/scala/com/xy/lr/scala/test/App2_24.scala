package com.xy.lr.scala.test

import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.HashMap

/**
  * Created by xylr on 16-4-14.
  * com.xy.lr.scala.test
  */
object App2_24  {
  def main(args : Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("App")
    val sc = new SparkContext(conf)

    val pairs = sc.makeRDD(Array(("a", 1), ("b", 2), ("a", 4), ("c", 5), ("a", 3)))
    type StringMap = HashMap[String, Int]

    val emptyMap = new StringMap {
      override def default(key : String) : Int = 0
    }

    val mergeElement : (StringMap, (String, Int)) => StringMap =
      (map, pairs) => {
        map(pairs._1) += pairs._2
        map
      }

    val mergeMaps : (StringMap, StringMap) => StringMap =
      (map1, map2) => {
        for ((key, value) <- map2) {
          map1(key) += value
        }
        map1
      }

    //类似map reduce
    val aggregateResult = pairs.aggregate(emptyMap)(mergeElement, mergeMaps)

  }
  }
