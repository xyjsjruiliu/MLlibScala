package com.xy.lr.scala.mllibScala.pagerank

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by xylr on 16-4-12.
  * com.xy.lr.scala.mllib
  */
object PageRank {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("PageRank")
    val sc = new SparkContext(conf)

    val links : RDD[(Char, Array[Char])] = sc.parallelize(Array(
      ('A', Array('D')), ('B', Array('A')), ('C', Array('A', 'B')), ('D', Array('A', 'C')))).map{
      x => (x._1, x._2)
    }.cache()

    var ranks : RDD[(Char, Double)] = sc.parallelize(Array(('A', 1.0), ('B', 1.0), ('C', 1.0), ('D', 1.0)))

    for (i <- 1 to 10) {
      val contribs = links.join(ranks).flatMap {
        case(url, (links, rank)) => links.map(dest => (dest, rank / links.size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }

    ranks.saveAsTextFile("data/pagerank")
  }
}
