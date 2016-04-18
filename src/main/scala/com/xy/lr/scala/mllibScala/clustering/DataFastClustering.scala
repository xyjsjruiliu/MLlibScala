package com.xy.lr.scala.mllibScala.clustering

/**
  * Created by xylr on 16-4-18.
  * com.xy.lr.scala.mllibScala.clustering
  */
class DataFastClustering {
  private var dc : Double = _

  def this(dc : Double) {
    this()
    this.dc = dc
  }

  def getDC() : Double = {
    this.dc
  }

  def reSetDC(dc : Double): Unit = {
    this.dc = dc
  }
}
