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
    val delta = sc.accumulator(0.toDouble, "delta")
    val alpha = sc.broadcast(0.85)
    
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
    
    var ranksRDD = adjList.map(t => {
      pageCount += 1
      if (t._2 == null || t._2.length() == 0) {
        danglingNodesCount += 1
      }
      (t._1, Double.NegativeInfinity)
    }).reduceByKey(_ + _)
    
    val count = ranksRDD.count()
    println("Ranks RDD count: " + count)
    
    val defaultRank = sc.broadcast(1 / pageCount.value.toDouble)
    val totalPages = sc.broadcast(pageCount.value)
    val totalDanglingNodes = sc.broadcast(danglingNodesCount.value)
    val alphaByPageCount = sc.broadcast(0.85 / pageCount.value)
        
    val i = 0
    for (i <- 0 until 2) {
      val pageRankRDD = adjList.join(ranksRDD)
      
      val fractionalRanksRDD = pageRankRDD.flatMap(page => {
        var outlinks = Array.empty[String]
        var adjNodes = page._2._1
        var fractionalRank = Double.NegativeInfinity
        
        if (adjNodes != null && adjNodes.length() > 0) {
          outlinks = page._2._1.split(";")
          
          var prevRank = defaultRank.value
          if (page._2._2 != Double.NegativeInfinity) {
            prevRank = page._2._2
          }
          
          fractionalRank = prevRank
          if (outlinks.length > 0) {
            fractionalRank = prevRank / outlinks.length
          } else {
            delta += prevRank;
          }
        }
        
        outlinks.map(outlink => (outlink, fractionalRank))
      }).reduceByKey(_ + _)
      
      val _delta = sc.broadcast(delta.value)
      
      ranksRDD = fractionalRanksRDD
                   .map(page => {                            
                     val x = _delta.value / totalPages.value
                     val y = 1 - alpha.value
                     val z = x + page._2
                     (page._1, alphaByPageCount.value + y * z)
                   })
                   .reduceByKey(_ + _)
      
      // Re-initialize delta to 0
      delta.setValue(0)
    }
    
    ranksRDD.saveAsTextFile("/home/ideepakkrishnan/Documents/pageRank/spark_op")    
    println( "Completed Page Rank calculation" )
  }

}
