package com.xy.lr.scala.spark.graphx

import java.io.PrintWriter

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.math.random

/**
  * Created by xylr on 16-4-6.
  * com.xy.lr.scala.mllib.graphx
  */
class PairDistance {
  private var conf : SparkConf = _
  private var sc : SparkContext = _


  /**
    * 初始化spark
    * @param appName
    * @param master
    */
  def this(appName : String, master : String){
    this()
    conf = new SparkConf().setAppName(appName).setMaster(master)
    sc = new SparkContext(conf)
  }

  /**
    * 计算pi
    * @param args
    */
  def cal(args : Array[String]): Unit = {
    val slices = if (args.length > 0) args(0).toInt else 2
    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
    val count = sc.parallelize(1 until n, slices).map { i =>
        val x = random * 2 - 1
        val y = random * 2 - 1
        if (x*x + y*y < 1) 1 else 0
      }.reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / n)
    sc.stop()
  }

  /**
    * 导入文件
    * @param fileName
    */
  def loadFile(fileName : String): ArrayBuffer[String] = {
    val file = Source.fromFile(fileName)
    val arrayBuffer = new ArrayBuffer[String]()

    //遍历文件
    for (line <- file.getLines()) {
      val temp = line.substring(0, line.lastIndexOf(","))
      println(temp)
      arrayBuffer.append(temp)
    }

    println(arrayBuffer.size)

    arrayBuffer
  }
}

object PairDistance {
  def main(args : Array[String]): Unit = {
    val pairDistance = new PairDistance("Spark Pi", "spark://localhost:7077")
    //导入文件
//    val list = pairDistance.loadFile("/home/xylr/Working/IdeaProjects/KnowLedgeBase/chineseword/")
  }
}
