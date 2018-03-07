import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// define the auctionschema using a case class
case class Auction(auctionid: String, bid: Float, bidtime: Float, bidder: String,
                   bidderrate: Integer, openbid: Float, price: Float, item: String, daystolive: Integer)

object EbayAuctionAnalysis {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\Work\\spark-1.6.1-bin-hadoop2.6")
    val conf = new SparkConf().setAppName("Movie Recommender").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // SQLContext entry point for working with structured data
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // This is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._

    // Import Spark SQL data types and Row.
    import org.apache.spark.sql._

    // load the data into a new RDD
    val ebayText: RDD[String] = sc.textFile("D:\\Work\\intellij-workspace\\ScalaSpark\\data\\spark-ebook\\ebay.csv")

    // Return the first element in this RDD
    println(ebayText.first())

    // function to parse input into Auction class
    def parseAuction(str: String): Auction = {
      val fields: Array[String] = str.split(",")
      Auction(fields(0), fields(1).toFloat, fields(2).toFloat, fields(3), fields(4).toInt, fields(5).toFloat, fields(6).toFloat, fields(7), fields(8).toInt)
    }

    // load the data from the users and movies data files into an RDD, use the map() transformation with the parse functions, and then call toDF() to create a DataFrame for the RDD.
    val auctionsDF: DataFrame = ebayText.map(parseAuction).toDF()

    // Display the top 20 rows of DataFrame
    auctionsDF.show()

    // Return the schema of this DataFrame
    auctionsDF.printSchema()

    /* QUERY DATAFRAME */
    sqlContext.sql("set spark.sql.shuffle.partitions=50")

    println("How many Auctions were held?")
    auctionsDF.select("auctionid").distinct.count

    println("How many bids per item?")
    auctionsDF.groupBy("auctionid", "item").count.show

    println("Get the auctions with closing price > 100")
    val highprice: DataFrame = auctionsDF.filter("price > 100")

    // display dataframe in a tabular format
    highprice.show()

    /* QUERY REGISTERED TEMP TABLE */
    // register the DataFrame as a temp table
    auctionsDF.registerTempTable("auction")

    println("How many bids per auction?")
    val bidsPerAuction = sqlContext.sql(
                  """SELECT auctionid, item, COUNT(bid)
                    |FROM auction
                    |GROUP BY auctionid, item
                  """.stripMargin)
    // display dataframe in a tabular format
    bidsPerAuction.show()

    println("Maximum Bid Price per auction?")
    val maxPricePerAuction = sqlContext.sql(
                  """SELECT auctionid, MAX(price)
                    |FROM auction
                    |GROUP BY item,auctionid
                  """.stripMargin)

    maxPricePerAuction.show()
  }
}
