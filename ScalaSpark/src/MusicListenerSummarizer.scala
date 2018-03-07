import java.util

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary
import org.apache.spark.{SparkConf, SparkContext}

// Import Spark SQL data types
import org.apache.spark.sql._

import org.apache.spark.mllib.stat.Statistics

object MusicListenerSummarizer {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\Work\\spark-1.6.1-bin-hadoop2.6")
    val conf = new SparkConf().setAppName("Music Customer Analysis").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // SQLContext entry point for working with structured data
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // load the ratings data into a RDD
    val tracksRDD = sc.textFile("D:\\Work\\intellij-workspace\\ScalaSpark\\data\\spark-ebook\\tracks.csv")
    val tracksParsedRDD = tracksRDD.map(rec => rec.split(",")).map(fields => (fields(1).toInt, (fields(2).toInt, fields(3), fields(4).toInt, fields(5)))).groupByKey(50)

    //tracksParsedRDD.take(2).foreach(println)

    def computeStatsByUser(tracks: Iterable[(Int, String, Int, String)]): Vector = {
      var mcount: Int = 0
      var morn: Int = 0
      var aft: Int = 0
      var eve: Int = 0
      var night: Int = 0

      var trackList: Set[Int] = Set[Int]()
      for ((trackid, dtime, mobile, zip) <- tracks.toList) {

        if (!trackList.contains(trackid)) {
          trackList += trackid
        }

        val d = dtime.split(" ")(0)
        val t = dtime.split(" ")(1)

        val hourOfDay: Int = t.split(":")(0).toInt
        mcount = mcount + mobile
        if (hourOfDay < 5) {
          night += 1
        } else if (hourOfDay < 12) {
          morn += 1
        } else if (hourOfDay < 17) {
          aft += 1
        } else if (hourOfDay < 22) {
          eve += 1
        } else {
          night += 1
        }
      }

      return Vectors.dense(trackList.size, morn, aft, eve, night, mcount)
    }

    // compute profile for each user
    val custdata = tracksParsedRDD.mapValues(computeStatsByUser)
    //custdata.take(2).foreach(println)

    // compute aggregate stats for entire track history
    val summary: MultivariateStatisticalSummary = Statistics.colStats(custdata.map(rec => rec._2))

    println("Summary Counts...")
    println(summary.count)  // a dense vector containing the count value for each column
    println("Summary Maxs...")
    println(summary.max)  // a dense vector containing the max value for each column
    println("Summary Mins...")
    println(summary.min)  // a dense vector containing the min value for each column
    println("Summary Means...")
    println(summary.mean)  // a dense vector containing the mean value for each column
    println("Summary Variances...")
    println(summary.variance)  // column-wise variance

    println("\nInterpreting the summary statistics...People Listen to highest number of songs in the Night!")
    println(f"Maximum Tracks Listened: By Any Customer = ${summary.max(0).toInt}%d, Morning = ${summary.max(1).toInt}%d, Afternoon = ${summary.max(2).toInt}%d, Evening = ${summary.max(3).toInt}%d, Night = ${summary.max(4).toInt}%d, On Mobile = ${summary.max(5).toInt}%d")
  }
}
