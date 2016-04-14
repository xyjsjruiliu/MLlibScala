package com.xy.lr.scala.spark

/*import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.mapreduce.Job*/
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by xylr on 16-4-14.
  * com.xy.lr.scala.spark
  */
object NewRDDToHBase {
  /*val conf = new SparkConf().setMaster("local[2]").setAppName("App")
  val sc = new SparkContext(conf)

  val job = new Job(sc.hadoopConfiguration)
  job.setOutputKeyClass(classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable])
  job.setOutputValueClass(classOf[org.apache.hadoop.hbase.client.Result])

  job.setOutputFormatClass(classOf[TableOutputFormat[
    org.apache.hadoop.hbase.io.ImmutableBytesWritable]])*/
}
