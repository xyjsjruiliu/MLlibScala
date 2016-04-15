package com.xy.lr.scala.spark.graphx

import java.io.{File, PrintWriter}

import com.xy.lr.java.tools.file.JFile
import com.xy.lr.scala.KBSourceData
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

  private var kBSourceDataRDD : RDD[KBSourceData] = _

  /**
    * 初始化spark
    * @param appName app name
    * @param master master url
    */
  def this(appName : String, master : String){
    this()
    /*conf = new SparkConf().setAppName(appName).setMaster(master)
    sc = new SparkContext(conf)*/
  }

  /*/**
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
  }*/

  /**
    * 导入文件
    * @param fileName file mulu
    */
  private def loadFile(fileName : String): ArrayBuffer[Long] = {
    val files = new File(fileName)
    var kBSourceDatas : ArrayBuffer[KBSourceData] = new ArrayBuffer[KBSourceData]()
    var list : ArrayBuffer[Long] = new ArrayBuffer[Long]()
    for (file <- files.listFiles()) {
      //新的数据
      val kBSourceData = new KBSourceData()
      val id = file.getName.substring(file.getName.indexOf("_") + 1,
        file.getName.indexOf(".txt"))
      kBSourceData.setId(id.toLong)
      list += id.toLong
//      val lineData = JFile.getAllLines(file).replace("\n", "")
      val lineData = getAllLines(file).replace("\n", "")//节点数据
      kBSourceData.setData(lineData)
      kBSourceDatas += kBSourceData
    }
    //转换成RDD
    kBSourceDataRDD = sc.parallelize(kBSourceDatas)
    list
  }

  private def loadFiles(fileName : String) : ArrayBuffer[Long] = {
    val file = Source.fromFile(fileName)
    var kBSourceDatas : ArrayBuffer[KBSourceData] = new ArrayBuffer[KBSourceData]()
    var list : ArrayBuffer[Long] = new ArrayBuffer[Long]()
    for (line <- file.getLines()) {
      val kBSourceData = new KBSourceData()
      val id = line.split("\t")(0)
      kBSourceData.setId(id.toLong)
      list += id.toLong

      val lineData = line.split("\t")(1)
      kBSourceData.setData(lineData)
      kBSourceDatas += kBSourceData
    }
    //转换成RDD
    kBSourceDataRDD = sc.parallelize(kBSourceDatas)
    list
  }

  private def getAllLines(file : File) : String = {
    val f = Source.fromFile(file)
    val lines = f.getLines()
    var temp = ""
    for (line <- lines) {
      temp += line
    }
    f.close()
    temp
  }

  /*private def filesTofile(): Unit = {
    val files = new File("/home/xylr/Working/IdeaProjects/KnowLedgeBase/chineseword/data/")
    val out = new PrintWriter("" +
      "/home/xylr/Working/IdeaProjects/KnowLedgeBase/chineseword/input.txt")

    for (file <- files.listFiles()) {
      val id = file.getName.substring(file.getName.indexOf("_") + 1,
        file.getName.indexOf(".txt"))
      val lineData = getAllLines(file).replace("\n", "")//节点数据

      out.println(id + "\t" + lineData)
    }
    out.close()
  }*/

  /**
    * 生成点集合
    * @return
    */
  private def makeVertex (): RDD[(VertexId, String)] = {
    val vertexRDD : RDD[(VertexId, String)] = kBSourceDataRDD.map(x => {
      (x.getIdList, x.getDataList)
    })

    vertexRDD
  }

  /**
    * 生成边集合
    * @param list
    * @return
    */
  private def makeEdge (list : ArrayBuffer[Long]): RDD[Edge[Double]] = {
    val edgeRDD : RDD[Edge[Double]] = kBSourceDataRDD.map(x => {
      (x.getIdList, 1L)
    }).flatMapValues(x => {
      list
    }).filter(x => {
      if(x._1 >= x._2) false
      else true
    }).map( x => {
      Edge(x._1, x._2, 0.0)
    } )

    edgeRDD
  }

  def makeGraph(fileName : String): Unit = {
    val list = loadFile(fileName)
    val graph : Graph[String, Double] = Graph(makeVertex(), makeEdge(list))
    graph.cache()

    graph.mapTriplets(triplet => {
      triplet.attr
    })


    /*println(graph.vertices.count())
    println(graph.edges.count())*/
  }
}

object PairDistance {
  def main(args : Array[String]): Unit = {
    val pairDistance = new PairDistance("Spark Pi", "spark://localhost:7077")

//    pairDistance.filesTofile()
    //导入文件
    /*val list = pairDistance.loadFile(
      "/home/xylr/Working/IdeaProjects/KnowLedgeBase/chineseword/data/")*/

//    pairDistance.makeGraph("/home/xylr/Working/IdeaProjects/KnowLedgeBase/chineseword/data/")
//    pairDistance.makeEdge(list)
  }
}
