package com.xy.lr.scala.mllibScala.clustering

import com.xy.lr.scala.spark.graphx.PairDistance

/**
  * Created by xylr on 16-4-15.
  * com.xy.lr.scala.mllibScala.clustering
  */
class FastClustering {
  private var pairDistance : PairDistance = _

  /*def this(){
    pairDistance = new PairDistance("Spark Pi", "local[2]")
  }*/

  def getGraph(): Unit = {
    val graph = pairDistance.makeGraph(
      "/home/xylr/Working/IdeaProjects/KnowLedgeBase/chineseword/test.txt"
    )
  }
}
