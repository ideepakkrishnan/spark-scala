package com.neu.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * @author ${user.name}
 */
object App {
  
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  def main(args : Array[String]) {
    println( "Starting average TMAX calculator" )
    val conf = new SparkConf()
               .setAppName("Average TMAX Calculator")
               .setMaster("local")
    
    val sc = new SparkContext(conf)
    
    val pairRDD = sc.textFile("/home/ideepakkrishnan/Documents/climateAnalysis/input")
                    .map(line => line.split(","))
                    .filter(fields => fields(2) == "TMAX")
                    .map(fields => (fields(0), Integer.parseInt(fields(3)))).persist()
                    
    val sumByKey = pairRDD.reduceByKey((sum, temp) => sum + temp)
    val countByKey = pairRDD.foldByKey(0)((count, temp) => count + 1)
    val sumCountByKey = sumByKey.join(countByKey)
    val avgByKey = sumCountByKey.map(info => (info._1, info._2._1 / info._2._2.toDouble))
    
    avgByKey.saveAsTextFile("/home/ideepakkrishnan/Documents/climateAnalysis/spark_op")
    
    println( "Completed average TMAX calculation" )
  }

}
