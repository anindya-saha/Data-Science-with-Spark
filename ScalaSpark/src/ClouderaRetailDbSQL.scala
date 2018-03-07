import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
//import com.typesafe.scalalogging.LazyLogging

import org.apache.spark.rdd.RDD


/*
mysql> describe categories;
+------------------------+-------------+------+-----+---------+----------------+
| Field                  | Type        | Null | Key | Default | Extra          |
+------------------------+-------------+------+-----+---------+----------------+
| category_id            | int(11)     | NO   | PRI | NULL    | auto_increment |
| category_department_id | int(11)     | NO   |     | NULL    |                |
| category_name          | varchar(45) | NO   |     | NULL    |                |
+------------------------+-------------+------+-----+---------+----------------+


mysql> describe customers;
+-------------------+--------------+------+-----+---------+----------------+
| Field             | Type         | Null | Key | Default | Extra          |
+-------------------+--------------+------+-----+---------+----------------+
| customer_id       | int(11)      | NO   | PRI | NULL    | auto_increment |
| customer_fname    | varchar(45)  | NO   |     | NULL    |                |
| customer_lname    | varchar(45)  | NO   |     | NULL    |                |
| customer_email    | varchar(45)  | NO   |     | NULL    |                |
| customer_password | varchar(45)  | NO   |     | NULL    |                |
| customer_street   | varchar(255) | NO   |     | NULL    |                |
| customer_city     | varchar(45)  | NO   |     | NULL    |                |
| customer_state    | varchar(45)  | NO   |     | NULL    |                |
| customer_zipcode  | varchar(45)  | NO   |     | NULL    |                |
+-------------------+--------------+------+-----+---------+----------------+

mysql> describe departments;
+-----------------+-------------+------+-----+---------+----------------+
| Field           | Type        | Null | Key | Default | Extra          |
+-----------------+-------------+------+-----+---------+----------------+
| department_id   | int(11)     | NO   | PRI | NULL    | auto_increment |
| department_name | varchar(45) | NO   |     | NULL    |                |
+-----------------+-------------+------+-----+---------+----------------+

mysql> describe orders;
+-------------------+-------------+------+-----+---------+----------------+
| Field             | Type        | Null | Key | Default | Extra          |
+-------------------+-------------+------+-----+---------+----------------+
| order_id          | int(11)     | NO   | PRI | NULL    | auto_increment |
| order_date        | datetime    | NO   |     | NULL    |                |
| order_customer_id | int(11)     | NO   |     | NULL    |                |
| order_status      | varchar(45) | NO   |     | NULL    |                |
+-------------------+-------------+------+-----+---------+----------------+

mysql> describe order_items;
+--------------------------+------------+------+-----+---------+----------------+
| Field                    | Type       | Null | Key | Default | Extra          |
+--------------------------+------------+------+-----+---------+----------------+
| order_item_id            | int(11)    | NO   | PRI | NULL    | auto_increment |
| order_item_order_id      | int(11)    | NO   |     | NULL    |                |
| order_item_product_id    | int(11)    | NO   |     | NULL    |                |
| order_item_quantity      | tinyint(4) | NO   |     | NULL    |                |
| order_item_subtotal      | float      | NO   |     | NULL    |                |
| order_item_product_price | float      | NO   |     | NULL    |                |
+--------------------------+------------+------+-----+---------+----------------+

mysql> describe products;
+---------------------+--------------+------+-----+---------+----------------+
| Field               | Type         | Null | Key | Default | Extra          |
+---------------------+--------------+------+-----+---------+----------------+
| product_id          | int(11)      | NO   | PRI | NULL    | auto_increment |
| product_category_id | int(11)      | NO   |     | NULL    |                |
| product_name        | varchar(45)  | NO   |     | NULL    |                |
| product_description | varchar(255) | NO   |     | NULL    |                |
| product_price       | float        | NO   |     | NULL    |                |
| product_image       | varchar(255) | NO   |     | NULL    |                |
+---------------------+--------------+------+-----+---------+----------------+
 */

case class Customer(customer_id: Int, customer_fname: String, customer_lname: String, customer_email: String, customer_password: String, customer_street: String, customer_city: String, customer_state: String, customer_zipcode: String)
case class Order(order_id: Int, order_date: String, order_customer_id: Int, order_status: String)
case class OrderItem(order_item_id: Int, order_item_order_id: Int, order_item_product_id: Int, order_item_quantity: Int, order_item_subtotal: Float, order_item_product_price: Float)
case class Product(product_id: Int, product_category_id: Int, product_name: String, product_description: String, product_price: Float, product_image: String)

// Using data frames (only works with spark 1.3.0 or later)
object ClouderaRetailDbSQL {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\Anindya\\Work\\spark-1.6.1-bin-hadoop2.6")
    val conf = new SparkConf().setAppName("ClouderaRetailDbSQL Exercises").setMaster("local[1]")
    val sc = new SparkContext(conf)

    //getTotalRecordsInOrdersTable(sc: SparkContext)
    //getAverageRevenuePerDay(sc: SparkContext)
    //getAverageRevenuePerOrder(sc: SparkContext)
    //getHighestPricedProduct(sc: SparkContext)
    //getCanceledOrdersWithTotalMoreThen1000(sc: SparkContext)
    sortingByKeyExamples(sc: SparkContext)
  }

  def getTotalRecordsInOrdersTable(sc: SparkContext): Unit = {

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    sqlContext.sql("set spark.sql.shuffle.partitions=10")

    // read orders.csv
    val ordersRDD = sc.textFile("D:\\Work\\intellij-workspace\\ScalaSpark\\data\\retail_db\\orders\\orders.csv")
      .map(_.split(","))
      .map(fields => Order(fields(0).toInt, fields(1), fields(2).toInt, fields(3)))
      .toDF()
    ordersRDD.registerTempTable("orders")

    // Get total number of records in Orders
    val sql = "select count(1) from orders"
    val totalOrders: RDD[Long] = sqlContext.sql(sql).map(row => (row.getLong(0)))

    totalOrders.collect().foreach(println)
  }

  def getAverageRevenuePerOrder(sc: SparkContext): Unit = {

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    sqlContext.sql("set spark.sql.shuffle.partitions=10")

    // read orders.csv and order_items.csv
    val ordersRDD = sc.textFile("D:\\Work\\intellij-workspace\\ScalaSpark\\data\\retail_db\\orders\\orders.csv")
      .map(_.split(","))
      .map(fields => Order(fields(0).toInt, fields(1), fields(2).toInt, fields(3)))
      .toDF()
    ordersRDD.registerTempTable("orders")

    val orderItemsRDD = sc.textFile("D:\\Work\\intellij-workspace\\ScalaSpark\\data\\retail_db\\order_items\\order_items.csv")
      .map(_.split(","))
      .map(fields => OrderItem(fields(0).toInt, fields(1).toInt, fields(2).toInt, fields(3).toInt, fields(4).toFloat, fields(5).toFloat))
      .toDF()
    orderItemsRDD.registerTempTable("order_items")

    val sql = "select sum(oi.order_item_subtotal) / count(distinct oi.order_item_order_id) " +
      "from orders o join order_items oi on o.order_id = oi.order_item_order_id"

    //val averageRevenuePerOrder = sqlContext.sql(sql)
    val averageRevenuePerOrder: RDD[Double] = sqlContext.sql(sql).map(row => row.getDouble(0))
    averageRevenuePerOrder.collect().foreach(println)
  }

  def getHighestPricedProduct(sc: SparkContext): Unit = {

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    sqlContext.sql("set spark.sql.shuffle.partitions=10")

    // read products.csv
    val productsRDD = sc.textFile("D:\\Work\\intellij-workspace\\ScalaSpark\\data\\retail_db\\products\\products.csv")
      .map(_.split(","))
      .map(fields => Product(fields(0).toInt, fields(1).toInt, fields(2), fields(3), fields(4).toFloat, fields(5)))
      .toDF()
    productsRDD.registerTempTable("products")

    // Get the highest priced product from products table
    val sql = "select p.* from products p where p.price = select max(q.price) from products q"
    val topProducts: RDD[Product] = sqlContext.sql(sql).map(row => Product(row.getInt(0), row.getInt(1), row.getString(2), row.getString(3), row.getFloat(4), row.getString(5)))
    topProducts.collect().foreach(println)
  }

  /*
  Join disparate datasets together using Spark
  Problem statement: Get the Revenue Revenue on daily basis.
  */
  def getAverageRevenuePerDay(sc: SparkContext): Unit = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    sqlContext.sql("set spark.sql.shuffle.partitions=10")

    // read orders.csv and order_items.csv
    val ordersRDD = sc.textFile("D:\\Work\\intellij-workspace\\ScalaSpark\\data\\retail_db\\orders\\orders.csv")
      .map(_.split(","))
      .map(fields => Order(fields(0).toInt, fields(1), fields(2).toInt, fields(3)))
      .toDF()
    ordersRDD.registerTempTable("orders")

    val orderItemsRDD = sc.textFile("D:\\Work\\intellij-workspace\\ScalaSpark\\data\\retail_db\\order_items\\order_items.csv")
      .map(_.split(","))
      .map(fields => OrderItem(fields(0).toInt, fields(1).toInt, fields(2).toInt, fields(3).toInt, fields(4).toFloat, fields(5).toFloat))
      .toDF()
    orderItemsRDD.registerTempTable("order_items")

    val sql = "select o.order_date, sum(oi.order_item_subtotal) / count(distinct oi.order_item_order_id) " +
              "from orders o join order_items oi on o.order_id = oi.order_item_order_id " +
              "group by o.order_date " +
              "order by o.order_date"

    //val averageRevenuePerDay = sqlContext.sql(sql)
    val averageRevenuePerDay: RDD[(String, Double)] = sqlContext.sql(sql).map(row => (row.getString(0), row.getDouble(1)))
    averageRevenuePerDay.collect().foreach(println)
  }

  /*
  Join & Filter
  Problem statement: Check if there are any CANCELED orders with amount greater than $1000
*/
  def getCanceledOrdersWithTotalMoreThen1000(sc: SparkContext): Unit = {

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    sqlContext.sql("set spark.sql.shuffle.partitions=10")

    // read orders.csv and order_items.csv
    val ordersRDD = sc.textFile("D:\\Work\\intellij-workspace\\ScalaSpark\\data\\retail_db\\orders\\orders.csv")
      .map(_.split(","))
      .map(fields => Order(fields(0).toInt, fields(1), fields(2).toInt, fields(3)))
      .toDF()
    ordersRDD.registerTempTable("orders")

    val orderItemsRDD = sc.textFile("D:\\Work\\intellij-workspace\\ScalaSpark\\data\\retail_db\\order_items\\order_items.csv")
      .map(_.split(","))
      .map(fields => OrderItem(fields(0).toInt, fields(1).toInt, fields(2).toInt, fields(3).toInt, fields(4).toFloat, fields(5).toFloat))
      .toDF()
    orderItemsRDD.registerTempTable("order_items")

    val sql = "select q.*  " +
              "from ( " +
              "       select o.order_id, sum(oi.order_item_subtotal) as order_total " +
              "       from orders o join order_items oi " +
              "       on o.order_id = oi.order_item_order_id " +
              "       where o.order_status = 'CANCELED' " +
              "       group by o.order_id " +
              "     ) q " +
              " where q.order_total >= 1000 " +
              " order by q.order_id"

    val canceledOrdersWithTotalMoreThen1000 = sqlContext.sql(sql)
    canceledOrdersWithTotalMoreThen1000.collect().foreach(println)
  }

  def sortingByKeyExamples(sc: SparkContext): Unit = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    sqlContext.sql("set spark.sql.shuffle.partitions=10")

    val productsRDD = sc.textFile("C:\\Anindya\\Work\\intellij-workspace\\ScalaSpark\\data\\retail_db\\products\\products.csv")
      .map(_.split(","))
      .map(fields => Product(fields(0).toInt, fields(1).toInt, fields(2), fields(3), fields(4).toFloat, fields(5)))
      .toDF()
    productsRDD.registerTempTable("products")

    // Sorting using queries
    // Global sorting and ranking
    //val sql = "select * from products order by product_price desc"

    // By key sorting
    // Using order by is not efficient, it serializes
    //val sql = "select * from products order by product_category_id, product_price"

    // Using distribute by sort by (to distribute sorting and scale it up)
    val sql = "select * from products distribute by product_category_id sort by product_price desc" // not working

    val products: RDD[Product] = sqlContext.sql(sql).map(row => Product(row.getInt(0), row.getInt(1), row.getString(2), row.getString(3), row.getFloat(4), row.getString(5)))
    products.collect().foreach(println)
  }
}
