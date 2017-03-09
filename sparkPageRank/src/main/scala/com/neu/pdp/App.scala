package com.neu.pdp

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.neu.pdp.resources.Util
import java.util.regex.Pattern

/**
 * @author ${user.name}
 */
object App {
  
  val namePattern = Pattern.compile("^([^~]+)$");
  
  def main(args : Array[String]) {
    println( "Starting Page Rank calculator" )
    val conf = new SparkConf()
               .setAppName("Page Rank")
               .setMaster("local")
    
    val sc = new SparkContext(conf)
    
    val pairRDD = sc.textFile("/home/ideepakkrishnan/Documents/pageRank/input")
                    .map(line => {
                      val delimIndex = line.indexOf(':')
                      val pageName = line.substring(0, delimIndex);
                      val pageContent = line.substring(delimIndex + 1)
                      (pageName, pageContent)
                    })
                    .filter(t => namePattern.matcher(t._1).find())
                    .map(t => {
                        val outlinks = Util.fetchOutlinks(t._2)
                        
                        // Emit adjacent list for this page
                        val outlinks_string = outlinks.reduce((a, b) => a + ";" + b)
                        (t._1, outlinks_string)
                    })
                    
    val adjList = pairRDD.reduceByKey((a, b) => a + "," + b)
    
    adjList.saveAsTextFile("/home/ideepakkrishnan/Documents/pageRank/spark_op")
    
    println( "Completed Page Rank calculation" )
  }

}
