package com.xy.lr.scala

import scala.collection.mutable.ArrayBuffer

/**
  * Created by xylr on 16-4-14.
  * com.xy.lr.scala
  */
class KBSourceData extends Serializable{
  private var idList : Long = _
  private var dataList : String = _
  idList = 0
  dataList = ""
  def setId(id : Long): Unit = {
    this.idList = id
  }
  def setData(data : String): Unit = {
    this.dataList = data
  }
  def getIdList : Long = {
    this.idList
  }
  def getDataList : String = {
    this.dataList
  }
}
