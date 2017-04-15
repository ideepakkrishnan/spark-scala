package com.neu.pdp.dataSampler

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.util.Random

/**
 * @author ${user.name}
 */
object App {
  
  def main(args : Array[String]) {
    
    if (args.length == 2) {
       /**
       * Initialize spark context and define accumulators
       */
      // Initialize job configuration
      val conf = new SparkConf()
                 .setAppName("Data Sampler")
      
      // Initialize job context
      val sc = new SparkContext(conf)
      
      // Read the input & output file path
      val inputPath = args(0)
      val outputPath = args(1)
      
      // Read the data and extract all pages and out-links
      val selectedRecordsRDD = sc.textFile(inputPath)
                      .map(line => {
                        val delimIndex = line.indexOf(',')
                        val stationId = line.substring(0, delimIndex)
                        val pageContent = line.substring(delimIndex)
                        
                        if (Random.nextFloat() * 100 < 3) {
                          (stationId, pageContent)
                        } else {
                          (stationId, "")
                        }
                      })
                      .filter(t => {t._2.length() > 1})
       
       // Write the sampled records to an output file
       selectedRecordsRDD.saveAsTextFile(outputPath)
    } else {
      println("Invalid run time arguments")
    }
  }

}
