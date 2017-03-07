package com.neu.test.spark_scala_test2

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * @author ${user.name}
 */
object App {
  
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  def main(args : Array[String]) {
    println( "Starting word count" )
    val conf = new SparkConf()
               .setAppName("Word Count")
               .setMaster("local")
    
    val sc = new SparkContext(conf)
    
    val textFile = sc.textFile("/home/ideepakkrishnan/Documents/wordCount/input/file01")
    val counts = textFile
                    .flatMap(line => line.split(" "))
                    .map(word => (word, 1))
                    .reduceByKey(_ + _)
    
    counts.saveAsTextFile("/home/ideepakkrishnan/Documents/wordCount/spark_op1")
    
    println( "Completed word count" )
  }

}
