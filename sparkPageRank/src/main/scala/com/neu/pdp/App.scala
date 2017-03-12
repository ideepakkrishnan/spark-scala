package com.neu.pdp

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import com.neu.pdp.resources.Util

import java.util.regex.Pattern
import java.io._

/**
 * Course: CS6240 - Parallel Data Processing
 * ----------------------------------------------------
 * Homework 4: Page Rank Calculator using Spark & Scala
 * ----------------------------------------------------
 * This application calculates the page rank for each
 * page in the data set and returns 100 pages having
 * the highest page ranks.
 * @author Deepak Krishnan
 * @email krishnan.d[at]husky[dot]neu[dot]edu
 */
object App {
  
  // Valid page name pattern for this application which is used
  // in the pre-processing step to filter out invalid pages
  val namePattern = Pattern.compile("^([^~]+)$");
  
  /**
   * Helper function to write data into a file
   * @param f Complete file path where contents are to be written
   */
  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }
  
  /**
   * Helper function for pre-processing step.
   * Reads the data set from specified path and extracts all
   * relevant pages and their out-links to generate a adjacency
   * list which is passed back to the caller as a RDD
   * @param sc The spark context
   * @param path The file system path where input files are stored
   * @return A RDD which stores the adjacency list
   */
  def generateAdjList(sc: SparkContext, path: String) : RDD[(String, String)] = {
    
    // Read the data and extract all pages and out-links
    val adjListRDD = sc.textFile(path)
                    .map(line => {
                      val delimIndex = line.indexOf(':')
                      val pageName = line.substring(0, delimIndex);
                      val pageContent = line
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
    
    // Emit a record for each out-link from each page so that we
    // can get hold of the dangling nodes
    val danglingNodesRDD = adjListRDD.flatMap(t => {
      t._2.split(";").map(pageName => (pageName, ""))
    })
    
    // Combine the RDD which contains all dangling nodes with the
    // RDD that contains the adjacency list for all pages in the
    // data set
    val allPagesRDD = adjListRDD.union(danglingNodesRDD)
    
    // Since the previous step might have resulted in multiple
    // records with same key, reduce by key
    val adjList = allPagesRDD.reduceByKey((a, b) => {      
      if (b != null && b.length() > 0) {
        a + ";" + b
      } else {
        a + b
      }
    })
    
    return adjList
  }
  
  /**
   * Calculates the new page rank for all pages using page rank
   * formula: [rank = (a / t) + (1 - a) * ((d / t) + s)] where:
   * a - alpha, t - total number of pages in the dataset,
   * d - delta and s - sum of fractional ranks passed on to
   * the current page by all pages that point to it.
   * @param rdd RDD storing the total incoming fractional ranks
   * 						for each page
   * @param delta Sum of page ranks of all dangling nodes from 
   * 							previous iteration
   * @param totalPages The total number of pages in the data set
   * @param alpha Value of alpha
   * @param alphaByPageCount Value of (alpha / totalPages)
   * @return A RDD storing the new page ranks for each page
   */
  def calculateNewRanks(
      rdd: RDD[(String, Double)],
      delta: Double,
      totalPages: Double,
      alpha: Double,
      alphaByPageCount: Double) : RDD[(String, Double)] = {
    val newRanksRDD = rdd.map(page => {                            
                        val x = delta / totalPages
                        val y = 1 - alpha
                        val z = x + page._2
                        (page._1, alphaByPageCount + y * z)
                      })
                      .reduceByKey(_ + _)
    
    return newRanksRDD
  }
  
  /**
   * The driver program
   * @param args The arguments for this application
   */
  def main(args : Array[String]) {
    /**
     * Initialize spark context and define accumulators
     */
    // Initialize job configuration
    val conf = new SparkConf()
               .setAppName("Page Rank")
               .setMaster("local")
    
    // Initialize job context
    val sc = new SparkContext(conf)
    
    // To store the total number of pages in the data set
    val pageCount = sc.accumulator(0, "total")
    
    // To store the number of dangling nodes in the data set
    val danglingNodesCount = sc.accumulator(0, "dangling")
    
    // To store the sum of page ranks for all dangling nodes
    val delta = sc.accumulator(0.toDouble, "delta")
    
    // To store the value of alpha required in page rank calculation
    val alpha = sc.broadcast(0.85)
    
    /**
     * Step 1: Pre-processing
     * Extracts all pages and their from the data set to generate
     * an adjacency list which can be used in actual page rank
     * calculation
     */
    val adjList = generateAdjList(sc, "/home/ideepakkrishnan/Documents/pageRank/input")
                    .persist()
    
    // Find the total number of pages and total number of dangling
    // nodes. Also initialize the default rank for each page to
    // negative infinity.
    var ranksRDD = adjList.map(t => {
      pageCount += 1
      if (t._2 == null || t._2.length() == 0) {
        danglingNodesCount += 1
      }
      (t._1, Double.NegativeInfinity)
    }).reduceByKey(_ + _)
    
    val count = ranksRDD.count() // Force the above transformation using an action
    
    /**
     * Initialize all broadcast variables required for page
     * rank calculation
     */
    val defaultRank = sc.broadcast(1 / pageCount.value.toDouble)
    val totalPages = sc.broadcast(pageCount.value)
    val totalDanglingNodes = sc.broadcast(danglingNodesCount.value)
    val alphaByPageCount = sc.broadcast(0.85 / pageCount.value)
    
    /**
     * Step 2: Page rank calculation
     */
    val i = 0
    for (i <- 0 until 10) {
      // Combine page rank from previous iteration with the 
      // adjacency list information for each page
      val pageRankRDD = adjList.join(ranksRDD)
      
      // Calculate the total incoming fractional rank for
      // each page and store it as an RDD
      val fractionalRanksRDD = pageRankRDD.flatMap(page => {
        var outlinks = Array.empty[String]
        var adjNodes = page._2._1
        var fractionalRank = Double.NegativeInfinity
        
        // Calculate the fractional rank to be sent across
        // to each outlink
        if (adjNodes != null && adjNodes.length() > 0) {
          outlinks = page._2._1.split(";")
          
          // Safety mechanism for first iteration. The
          // page rank for each page is considered to
          // be (1 / total number of pages)
          var prevRank = defaultRank.value
          if (page._2._2 != Double.NegativeInfinity) {
            // Means that this is not the first iteration
            prevRank = page._2._2
          }
          
          fractionalRank = prevRank
          if (outlinks.length > 0) {
            fractionalRank = prevRank / outlinks.length
          } else {
            // Means that we are dealing with a dangling
            // node. Add this page rank to the global 
            // accumulator since we need it for calculating
            // the new page rank
            delta += prevRank;
          }
        }
        
        // Dispatch the fractional rank to each outlink
        outlinks.map(outlink => (outlink, fractionalRank))
      }).reduceByKey(_ + _) // Add incoming fractional ranks
      
      val _delta = sc.broadcast(delta.value)
      
      // Calculate the new page rank of each page for
      // this iteration
      ranksRDD = calculateNewRanks(
                   fractionalRanksRDD,
                   _delta.value,
                   totalPages.value,
                   alpha.value,
                   alphaByPageCount.value)
      
      // Re-initialize delta to 0
      delta.setValue(0)
    }
    
    /**
     * Step 3: Top-K
     * Find the 100 pages with highest page ranks and write the
     * list into an output file 
     */
    val top100 = ranksRDD.top(100)(Ordering[Double].on(page => page._2))
    
    printToFile(new File("/home/ideepakkrishnan/Documents/pageRank/spark_op.csv")) { p =>
      top100.foreach(t => p.println(t._1 + "," + t._2))
    }
    
    println( "Completed Page Rank calculation" )
  }

}
