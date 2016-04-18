package com.xy.lr.scala.mllibScala.clustering

import com.xy.lr.scala.spark.graphx.PairDistance
import org.apache.spark.Accumulator
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
  * Created by xylr on 16-4-15.
  * com.xy.lr.scala.mllibScala.clustering
  */
class FastClustering(master : String, appName : String, fileName : String) extends Serializable{
  @transient private var pairDistance : PairDistance = _

  private var graph : Graph[(Double, Double), Double] = _

  pairDistance = new PairDistance(appName, master)

  graph = getGraph(fileName).mapVertices((vertexId, attr) => {
    (0.0, 0.0)
  })

  /**
    *
    * @return
    */
  private def getGraph(file : String): Graph[String, Double] = {
    val graph = pairDistance.makeGraph(
      file
    )
    graph
  }

  /**
    * 计算横截断距离
    * @return
    */
  def findDC(): Double = {
    var tmpMax : Broadcast[Double] = pairDistance.getMax(graph)
    var tmpMin : Broadcast[Double] = pairDistance.getMin(graph)

    var tmpDC : Broadcast[Double] =
      pairDistance.createBroadCast(0.5 * (tmpMax.value + tmpMin.value))

    val sampleSize = graph.vertices.count()

    for (iteration <- 1 to 100) {//迭代100次
      //spark累加变量
      var neighbourNum : Accumulator[Int] = pairDistance.createAccumulator(0)
      //计算样本距离小于截断距离样本点的个数
      graph.edges.map(x => {
        if (x.attr < tmpDC.value) neighbourNum += 2
      }).count()

      //所占的比率
      val neighborPercentage = pairDistance.createBroadCast(
        neighbourNum.value / Math.pow(sampleSize, 2))
      if (neighborPercentage.value >= 0.01 && neighborPercentage.value <= 0.02)//返回结果
        tmpDC.value
      if (neighborPercentage.value > 0.02) {//更新计算距离
        tmpMax = pairDistance.createBroadCast(tmpDC.value)
        tmpDC = pairDistance.createBroadCast(0.5 * (tmpMax.value + tmpMin.value))
      }
      if (neighborPercentage.value < 0.01) {//更新计算距离
        tmpMin = pairDistance.createBroadCast(tmpDC.value)
        tmpDC = pairDistance.createBroadCast(0.5 * (tmpMax.value + tmpMin.value))
      }
    }
    tmpDC.value
  }

  def calRho(@transient dcThreshold : Double): Unit = {
    val dcBroadCast = pairDistance.createBroadCast(dcThreshold)
    //首先求子图, 子图中所有的边都小于截断距离
    val subGraph = graph.subgraph(epred = x => {
      x.attr < dcBroadCast.value
    })
  }

  def calDelta(): Unit = {
  }
}
object FastClustering {
  def main(args : Array[String]): Unit = {
    val fastClustering = new FastClustering("local[2]", "FastClustering",
      "/home/xylr/Working/IdeaProjects/KnowLedgeBase/chineseword/test.txt")
    println(fastClustering.findDC())
//    val dc = fastClustering.findDC()

//    fastClustering.calRho(dc)
//    fastClustering.calDelta()

  }
}
