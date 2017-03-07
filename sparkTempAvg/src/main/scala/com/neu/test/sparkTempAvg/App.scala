package com.neu.test.sparkTempAvg

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * @author ${user.name}
 */
object App {
  
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  def main(args : Array[String]) {
    println( "Starting temperature average calculator" )
    val conf = new SparkConf()
               .setAppName("Temperature Average Calculator")
               .setMaster("local")
    
    val sc = new SparkContext(conf)
    
    val input = sc.textFile("/home/ideepakkrishnan/Documents/climateAnalysis/input/1820.csv")
    val tMaxReadings = input
                    .map(line => line.split(","))
                    .filter(fields => fields(2) == "TMAX").persist()
    
    val tMaxCount = tMaxReadings.count()
    
    val totalTMax = tMaxReadings
                    .map(fields => Integer.parseInt(fields(3)))
                    .reduce((sum, temp) => sum + temp)
    
    println( "Total average TMax: " + totalTMax / tMaxCount )
    
    println( "Completed temperature average calculation" )
  }

}
