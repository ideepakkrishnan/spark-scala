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
    
    // Define accumulators
    val pageCount = sc.accumulator(0, "total")
    val danglingNodesCount = sc.accumulator(0, "dangling")
    
    val adjListRDD = sc.textFile("/home/ideepakkrishnan/Documents/pageRank/input")
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
                        if (outlinks != null && outlinks.length > 0) {
                          val outlinks_string = outlinks.reduce((a, b) => a + ";" + b)
                          (t._1, outlinks_string)
                        } else {
                          (t._1, "")
                        }
                    })
    
    val danglingNodesRDD = adjListRDD.flatMap(t => {
      t._2.split(";").map(pageName => (pageName, ""))
    })
    
    val allPagesRDD = adjListRDD.union(danglingNodesRDD)
                    
    val adjList = allPagesRDD.reduceByKey((a, b) => {      
      if (b != null && b.length() > 0) {
        a + ";" + b
      } else {
        a + b
      }
    }).persist()
    
    adjList.map(t => {
      pageCount += 1
      if (t._2 == null || t._2.length() == 0) {
        danglingNodesCount += 1
      }      
    }).count()
    
    adjList.saveAsTextFile("/home/ideepakkrishnan/Documents/pageRank/spark_op")
    println("Total page count: " + pageCount)
    println("Dangling Node count: " + danglingNodesCount)
    println( "Completed Page Rank calculation" )
  }

}
