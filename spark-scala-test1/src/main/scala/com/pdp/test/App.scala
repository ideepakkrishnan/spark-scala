package com.pdp.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * @author ${user.name}
 */
object App {
  
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  def main(args : Array[String]) {
    val conf = new SparkConf()
               .setAppName("Simple does nothing")
               .setMaster("local")
               
    val sc = new SparkContext(conf)  
    
    val list = List(1,2)
    
    val simpleRDD = sc.parallelize(list)
    
    simpleRDD.foreach { x => println(x) }
  }

}
