package com.xy.lr.scala.mllibScala.clustering

import com.xy.lr.scala.spark.graphx.PairDistance
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
    *
    * @return
    */
  def findDC(): Double = {
    @transient var tmpMax : Double = pairDistance.getMax(graph)
    @transient var tmpMin : Double = pairDistance.getMin(graph)
    @transient var dc = 0.5 * (tmpMax + tmpMin)

    @transient var dataDC : RDD[Double] =
      pairDistance.getInitDC(tmpMax, tmpMin).map(x => {
        x.getDC()
      })

    @transient val entrySet = graph.edges
    @transient val sampleSize = graph.vertices.count()

    for (iteration <- 1 to 100) {
      var neighbourNum = pairDistance.createAccumulator()

      entrySet.map(x => {
        if (x.attr < dc) neighbourNum += 2
      })

//      println(iteration + "\t" +neighbourNum)
      val neighborPercentage = neighbourNum.value / Math.pow(sampleSize, 2)

      if (neighborPercentage >= 0.01 && neighborPercentage <= 0.02){
        return dc
      }

      if (neighborPercentage > 0.02) {
        tmpMax = dc
        dc = 0.5 * (tmpMax + tmpMin)
      }
      if (neighborPercentage < 0.01) {
        tmpMin = dc
        dc = 0.5 * (tmpMax + tmpMin)
      }
    }

    dc
  }

  def calRho(@transient dcThreshold : Double): Unit = {
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
