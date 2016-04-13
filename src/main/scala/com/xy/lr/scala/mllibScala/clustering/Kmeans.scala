package com.xy.lr.scala.mllibScala.clustering

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by xylr on 16-4-13.
  * com.xy.lr.scala.mllibScala.clustering
  */
object Kmeans{
  def main(args : Array[String]): Unit ={
    if(args.length != 4){
      System.err.println("Usage : $SPARK_HOME/bin/spark-submit \n\t--class com.xy.lr.spark.myMLlib.MyKmeans "
        + "\n\tmyMLlib-1.0-SNAPSHOT.jar \n\tinputFilePath \n\tnumClusters \n\tnumIterations \n\toutputFilePath")
      System.exit(0)
    }
    val sparkConf = new SparkConf().setAppName("Kmeans").setMaster("local[2]")
    val sparkContext = new SparkContext(sparkConf)

    //原始RDD
    val rdd = sparkContext.textFile(args(0))

    //向量标签和向量RDD
    val keyValueRDD : RDD[(String, String)] = rdd.map( x => {
      val spl = x.split("\t")
      val word = spl(0)
      val vector = spl(2)
      //标签，向量
      (word, vector)
    })

    //训练RDD
    val labeledPointRDD : RDD[Vector] = keyValueRDD.map( x => {
      val a = new ArrayBuffer[Double]()
      x._2.split(" ").map( y => {
        a += y.toDouble
      })
      //生成向量
      val b = Vectors.dense(a.toArray)
      b
    })

    //聚类中心个数
    val numClusters = args(1).toInt
    //迭代次数
    val numIterations = args(2).toInt
    //训练
    val clusters : KMeansModel = KMeans.train(labeledPointRDD, numClusters, numIterations)

    /*
    val WSSSE = clusters.computeCost(labeledPointRDD)
    println("Within Set Sum of Squared Errors = " + WSSSE)
    */

    clusters.predict(labeledPointRDD).collect().foreach(println(_))

    //预测类别
    val predictRDD : RDD[String] = keyValueRDD.map( x => {
      val a = new ArrayBuffer[Double]()
      x._2.split(" ").map( y => {
        a += y.toDouble
      })
      //生成向量
      val b = Vectors.dense(a.toArray)
      (x._1, b)
    }).map( x => {
      val i = clusters.predict(x._2)
      println(x._1 + "\t" + i)
      x._1+ "\t" +i
    })

    predictRDD.saveAsTextFile(args(3))

  }
}
