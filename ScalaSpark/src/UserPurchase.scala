import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
//import com.typesafe.scalalogging.LazyLogging

import org.apache.spark.rdd.RDD

object UserPurchase {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\Anindya\\Work\\spark-1.6.1-bin-hadoop2.6")
    val conf = new SparkConf().setAppName("User Purchase").setMaster("local[2]")
    val sc = new SparkContext(conf)

    //val infile = "D:\\Work\\intellij-workspace\\ScalaSpark\\data\\wordcount.txt"
    val infile = args{0}

    // we take the raw data in CSV format and convert it into a set of records of the form (user, product, price)
    val purchaseDataRDD = sc.textFile(infile).map(line => line.split(","))

    // let's count the number of purchases
    val numPurchases = purchaseDataRDD.count()

    // let's count how many unique users made purchases
    val uniqueUsers = purchaseDataRDD.map(rec => rec{0}).distinct().count()

    // let's sum up our total revenue
    val totalRevenue = purchaseDataRDD.map(rec => rec{2}.toDouble).reduce((a, b) => a + b)

    // let's find our most popular product
    // first we map the data to records of (product, 1)
    // then we call a reduceByKey operation
    val productPurchaseCount = purchaseDataRDD.map(rec => (rec{1}, 1)).reduceByKey((x, y) => x + y)

    // let's find the most popular product
    val mostPopular = productPurchaseCount.map(tuple => (tuple._2, tuple._1)).sortByKey(false).take(1)

    // print everything out
    println("Total purchases: " + numPurchases);
    println("Unique users: " + uniqueUsers);
    println("Total revenue: " + totalRevenue);
    println(f"Most popular product: ${mostPopular{0}._2}%s with ${mostPopular{0}._1}%d purchases");
  }
}
