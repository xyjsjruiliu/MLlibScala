package com.xy.lr.scala.mllibScala.clustering

import com.xy.lr.scala.spark.graphx.PairDistance
import org.apache.spark.graphx.Graph

/**
  * Created by xylr on 16-4-15.
  * com.xy.lr.scala.mllibScala.clustering
  */
class FastClustering(master : String, appName : String, fileName : String) extends Serializable{
  @transient private var pairDistance : PairDistance = _

  private var graph : Graph[String, Double] = _

  pairDistance = new PairDistance(appName, master)

  graph = getGraph(fileName)

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

  def findDC(): Double = {
    @transient var tmpMax : Double = pairDistance.getMaxDistance(graph)
    @transient var tmpMin : Double = pairDistance.getMinDistance(graph)

    @transient var dc = 0.5 * (tmpMax + tmpMin)
    println(dc)

    @transient val entrySet = graph.edges.collect()

    println(graph.edges.count())
    @transient val sample = graph.vertices.count()

    for (iteration <- 1 to 100) {
      var neighbourNum : Int = 0

      entrySet.foreach(x => {
        if (x.attr < dc) neighbourNum += 2
      })

      println(iteration + "\t" +neighbourNum)
      val neighborPercentage = neighbourNum / Math.pow(sample, 2)

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
}
object FastClustering {
  def main(args : Array[String]): Unit = {
    val fastClustering = new FastClustering("local[2]", "FastClustering",
      "/home/xylr/Working/IdeaProjects/KnowLedgeBase/chineseword/test.txt")
    println(fastClustering.findDC())

  }
}
