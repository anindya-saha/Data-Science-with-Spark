import org.apache.spark.{SparkConf, SparkContext}
//import com.typesafe.scalalogging.LazyLogging

object ClouderaRetailDb {

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

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\Anindya\\Work\\spark-1.6.1-bin-hadoop2.6")
    val conf = new SparkConf().setAppName("ClouderaRetailDb Exercises").setMaster("local[1]")
    val sc = new SparkContext(conf)

    //getTotalRecordsInOrdersTable(sc: SparkContext)
    //getAverageRevenuePerOrder(sc: SparkContext)
    //getHighestPricedProduct(sc: SparkContext)
    //getHighestRevenueEarningProduct(sc: SparkContext)
    //getOrdersCountByStatus(sc: SparkContext)
    //countOrdersByOrderDateAndStatus(sc: SparkContext)
    //getAverageRevenuePerDay(sc: SparkContext)
    //getCustomerIdWithMaxRevenuePerDay(sc: SparkContext)
    //globalFilterExamples(sc: SparkContext)
    //getCanceledOrdersWithTotalMoreThen1000(sc: SparkContext)
    //globalSortingExamples(sc: SparkContext)
    //sortingByKeyExamples(sc: SparkContext)
    rankingByKeyExamples(sc: SparkContext)


  }


  def getTotalRecordsInOrdersTable(sc: SparkContext): Unit = {

    // read orders.csv and order_items.csv
    val ordersRDD = sc.textFile("D:\\Work\\intellij-workspace\\ScalaSpark\\data\\retail_db\\orders\\orders.csv")

    // Get total number of records in Orders
    val totalOrders = ordersRDD.count()
    print(f"totalOrders = $totalOrders%d")
  }

  def getAverageRevenuePerOrder(sc: SparkContext): Unit = {

    // read orders.csv and order_items.csv
    val ordersRDD = sc.textFile("D:\\Work\\intellij-workspace\\ScalaSpark\\data\\retail_db\\orders\\orders.csv")
    val orderItemsRDD = sc.textFile("D:\\Work\\intellij-workspace\\ScalaSpark\\data\\retail_db\\order_items\\order_items.csv")

    // Get total revenue from order_items
    val totalRevenueFromOrderItems = orderItemsRDD.map(rec => rec.split(",")(4).toDouble).reduce((a, b) => a + b)
    print(f"total Revenue From Order Items = $totalRevenueFromOrderItems%f")

    // Get average revenue per order
    val orderCount = orderItemsRDD.map(rec => rec.split(",")(1).toInt).distinct().count()
    val avgRevenue = totalRevenueFromOrderItems / orderCount
    printf("avgRevenue = %f", avgRevenue)
  }

  def getHighestPricedProduct(sc: SparkContext): Unit = {

    // Get the highest priced product from products table
    val productsRDD = sc.textFile("D:\\Work\\intellij-workspace\\ScalaSpark\\data\\retail_db\\products\\products.csv")
    val productsMap = productsRDD.map(rec => rec)

    val topProducts = productsMap.reduce((prod_a, prod_b) => if(prod_a.split(",")(4).toFloat >= prod_b.split(",")(4).toFloat) prod_a else prod_b)
    print(topProducts)
  }

  def getHighestRevenueEarningProduct(sc: SparkContext): Unit = {

    val orderItemsRDD = sc.textFile("D:\\Work\\intellij-workspace\\ScalaSpark\\data\\retail_db\\order_items\\order_items.csv")
    val orderItemsMap = orderItemsRDD.map(rec => (rec.split(",")(2).toInt, rec.split(",")(4).toDouble))

    // fetch the highest revenue earning product id
    val revenuePerProduct = orderItemsMap.reduceByKey((subtotal_a, subtotal_b) => subtotal_a + subtotal_b)
    val topRevenueProduct = revenuePerProduct.reduce((prod_rev_a,prod_rev_b) => if(prod_rev_a._2 >= prod_rev_b._2) prod_rev_a else prod_rev_b)
    print(topRevenueProduct)

    // join with the actual product data to get the product name
    val productsRDD = sc.textFile("D:\\Work\\intellij-workspace\\ScalaSpark\\data\\retail_db\\products\\products.csv")
    val productsRDDMap = productsRDD.map(rec => (rec.split(",")(0).toInt, rec))
    productsRDDMap.filter(rec => rec._1 == topRevenueProduct._1).map(rec => rec._2).collect().foreach(println)
  }

  /*
  Count Number of Orders by status using different by key operations
  */
  def getOrdersCountByStatus(sc: SparkContext): Unit = {

    // Using countByKey
    val ordersRDD = sc.textFile("D:\\Work\\intellij-workspace\\ScalaSpark\\data\\retail_db\\orders\\orders.csv")

    println("Using countByKey...")
    var ordersMap = ordersRDD.map(rec => (rec.split(",")(3), 0)) //with countByKey you can give any value e.g. 0
    ordersMap.countByKey().foreach(println)

    // Using groupByKey. groupByKey is not efficient in this case, as the size of the output is very small compared to size of input data.
    ordersMap = ordersRDD.map(rec => (rec.split(",")(3), 1)) //with groupByKey you have to some value other than 0
    println("Using groupByKey...")
    ordersMap.groupByKey().map(rec => (rec._1, rec._2.sum)).collect().foreach(println)

    // Using reduceByKey. It uses combiner internally. Input data and output data for reduceByKey need to be of same type. Combiner is implicit and uses the reduce logic.
    ordersMap = ordersRDD.map(rec => (rec.split(",")(3), 1))
    println("Using reduceByKey...")
    ordersMap.reduceByKey((acc, value) => acc + value).collect().foreach(println)

    // Using combineByKey
    // http://abshinn.github.io/python/apache-spark/2014/10/11/using-combinebykey-in-apache-spark/
    ordersMap = ordersRDD.map(rec => (rec.split(",")(3), 1))
    println("Using combineByKey...")
    // In combineByKey we MUST SPECIFY datatype, it will NOT be inferred
    // ordersMap.combineByKey(value => 1, (acc, value) => acc + value, (acc1, acc2) => acc1 + acc2).collect().foreach(println) // this is wrong, no datatype specified
    ordersMap.combineByKey(value => 1, (acc: Int, value: Int) => acc + value, (acc1: Int, acc2: Int) => acc1 + acc2).collect().foreach(println)

    // Using aggregateByKey
    ordersMap = ordersRDD.map(rec => (rec.split(",")(3), 1))
    println("Using aggregateByKey...")
    // In aggregateByKey if we don't specify datatype it will be inferred from parameter in 1st parenthesis
    //ordersMap.aggregateByKey(0)((acc: Int, value: Int) => acc + value, (acc1: Int, acc2: Int) => acc1 + acc2).collect().foreach(println)
    ordersMap.aggregateByKey(0)((acc, value) => acc + value, (acc1, acc2) => acc1 + acc2).collect().foreach(println) // this is correct too
  }

  /*  Number of orders by order date and order status */
  def countOrdersByOrderDateAndStatus(sc: SparkContext): Unit = {

    // read orders.csv
    val ordersRDD = sc.textFile("D:\\Work\\intellij-workspace\\ScalaSpark\\data\\retail_db\\orders\\orders.csv")
    val ordersMap = ordersRDD.map(rec => ((rec.split(",")(1), rec.split(",")(3)), 1))

    // Using countByKey
    ordersMap.countByKey().foreach(println)

    // Using reduceByKey
    ordersMap.reduceByKey((a, b) => a + b).foreach(println)
  }

  /*
  Join disparate datasets together using Spark
  Problem statement: Get the Revenue Revenue on daily basis.
  */
  def getAverageRevenuePerDay(sc: SparkContext): Unit = {

    // read orders.csv and order_items.csv
    val ordersRDD = sc.textFile("D:\\Work\\intellij-workspace\\ScalaSpark\\data\\retail_db\\orders\\orders.csv")
    val orderItemsRDD = sc.textFile("D:\\Work\\intellij-workspace\\ScalaSpark\\data\\retail_db\\order_items\\order_items.csv")

    // Apply map function to get order_id as key and order_date as value from orders table.
    val ordersMap = ordersRDD.map(rec => (rec.split(",")(0).toInt, rec.split(",")(1)))

    // Apply map function to get order_item_order_id as key and order_item_subtotal as value from order_items table.
    val orderItemsMap = orderItemsRDD.map(rec => (rec.split(",")(1).toInt, rec.split(",")(4).toFloat))

    // Join data sets using spark transformation join. ordersJoinOrderItems will represent a tuple. Key is join column (order_id) value is a
    // tuple with corresponding orders record as first element and order_items record as second element
    val ordersJoinOrderItems = ordersMap.join(orderItemsMap)
    //ordersJoinOrderItems.take(25).foreach(println)

    // Get revenue for each order_id per day i.e order_date. output will be a tuple ((order_date, order_id) , order_total) from each record
    val revenuePerDayPerOrder = ordersJoinOrderItems.map(rec => ((rec._2._1, rec._1), rec._2._2)).reduceByKey((a, b) => a + b)
    //revenuePerDayPerOrder.take(25).foreach(println)

    // Discard the order_id. output will be a tuple (order_date, order_total) from each record
    val revenuePerDayPerOrderMap = revenuePerDayPerOrder.map(rec => (rec._1._1, rec._2))

    // Using aggregateByKey. Get count of orders and sum of revenues for each order_date. output will be a tuple (order_date, (total_revenue, order_count))
    //val revenuePerDay = revenuePerDayPerOrderMap.aggregateByKey((0.0, 0))((acc, value) => (acc._1 + value, acc._2 + 1), (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))


    // Using combineByKey deserves special treatment because we have seen earlier that we must specify datatype of the parameters, it will not be inferred
    // for an in-depth example http://codingjunkie.net/spark-combine-by-key/
    // below is an example when tuples are used how their datatypes can be defined
    val revenuePerDay = revenuePerDayPerOrderMap.combineByKey((value: Float) => (value , 1), (acc: (Float, Int), value: Float) => (acc._1 + value, acc._2 + 1), (acc1: (Float, Int), acc2: (Float, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
    //revenuePerDay.take(25).foreach(println)

    val avgRevenuePerDay = revenuePerDay.map(rec => (rec._1, rec._2._1 / rec._2._2))
    avgRevenuePerDay.sortByKey().collect().foreach(println)
  }

  /*
  Join disparate datasets together using Spark
  Problem statement: Get the Customer Id with max revenue on daily basis.
  */
  def getCustomerIdWithMaxRevenuePerDay(sc: SparkContext): Unit = {

    // read orders.csv and order_items.csv
    val ordersRDD = sc.textFile("D:\\Work\\intellij-workspace\\ScalaSpark\\data\\retail_db\\orders\\orders.csv")
    val orderItemsRDD = sc.textFile("D:\\Work\\intellij-workspace\\ScalaSpark\\data\\retail_db\\order_items\\order_items.csv")

    // Apply map function to get order_id as key and entire record as value from orders table.
    val ordersMap = ordersRDD.map(rec => (rec.split(",")(0).toInt, rec))

    // Apply map function to get order_item_order_id as key and entire record as value from order_items table.
    val orderItemsMap = orderItemsRDD.map(rec => (rec.split(",")(1).toInt, rec))

    // Join data sets using spark transformation join. ordersJoinOrderItems will represent a tuple. Key is join column (order_id) value is a
    // tuple with corresponding orders record as first element and order_items record as second element
    val ordersJoinOrderItems = ordersMap.join(orderItemsMap)
    //ordersJoinOrderItems.take(25).foreach(println)

    // Get revenue for each customer_id per day i.e order_date. output will be a tuple ((order_date, customer_id) , order_total) from each record
    val revenuePerDayPerCustomer = ordersJoinOrderItems.map(rec => ((rec._2._1.split(",")(1), rec._2._1.split(",")(2).toInt), rec._2._2.split(",")(4).toFloat)).reduceByKey((a, b) => a + b)
    //revenuePerDayPerCustomer.take(25).foreach(println)

    // Rearrange the dataset to create output tuple (order_date, (customer_id, order_total)) from each record
    val revenuePerDayPerCustomerMap = revenuePerDayPerCustomer.map(rec => (rec._1._1, (rec._1._2, rec._2)))

    // Using reduceByKey. Apply reduceByKey to get customer with maximum revenue per day.
    //val revenuePerDay = revenuePerDayPerOrderMap.aggregateByKey((0.0, 0))((acc, value) => (acc._1 + value, acc._2 + 1), (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
    println("Top Customer Id per day...")
    val topCustomerPerDayByRevenue = revenuePerDayPerCustomerMap.reduceByKey((acc, value) => if (acc._2 >= value._2) acc else value)
    topCustomerPerDayByRevenue.sortByKey().collect().foreach(println)

    val customersRDD = sc.textFile("D:\\Work\\intellij-workspace\\ScalaSpark\\data\\retail_db\\customers\\customers.csv")
    val customersMap = customersRDD.map(rec => (rec.split(",")(0).toInt, rec))

    println("Fetching the customer details by joining with customer table...")
    topCustomerPerDayByRevenue
      .map(rec => (rec._2._1, (rec._1, rec._2._2)))
      .join(customersMap)
      .map(rec => (rec._2._1._1, (rec._2._2.split(",")(0) + ": " + rec._2._2.split(",")(1) + " " + rec._2._2.split(",")(2), rec._2._1._2)))
      .sortByKey()
      .collect()
      .foreach(println)
  }

  /* Global Filter */
  def globalFilterExamples(sc: SparkContext): Unit = {

    // read orders.csv and order_items.csv
    val ordersRDD = sc.textFile("D:\\Work\\intellij-workspace\\ScalaSpark\\data\\retail_db\\orders\\orders.csv")

    // get all COMPLETED orders
    //ordersRDD.filter(rec => rec.split(",")(3).equals("COMPLETE")).collect().foreach(println)

    // get all PENDING orders
    //ordersRDD.filter(rec => rec.split(",")(3).contains("PENDING")).collect().foreach(println)

    // get all orders with orderid > 100
    //ordersRDD.filter(rec => rec.split(",")(0).toInt > 100).collect().foreach(println)

    // get all orders with orderid > 100 or with status PENDING
    // ordersRDD.filter(rec => rec.split(",")(0).toInt > 100 || rec.split(",")(3).contains("PENDING")).collect().foreach(println)

    // get all orders with orderid > 1000 and with status PENDING or CANCELED
    //ordersRDD.filter(rec => rec.split(",")(0).toInt > 1000 && (rec.split(",")(3).contains("PENDING") || rec.split(",")(3).equals("CANCELED"))).collect().foreach(println)

    // get all orders with orderid > 1000 and with status not COMPLETE
    ordersRDD.filter(rec => rec.split(",")(0).toInt > 1000 && !rec.split(",")(3).equals("COMPLETE")).collect().foreach(println)
  }

  /*
  Join & Filter
  Problem statement: Check if there are any CANCELED orders with amount greater than $1000
*/
  def getCanceledOrdersWithTotalMoreThen1000(sc: SparkContext): Unit = {

    // read orders.csv and order_items.csv
    val ordersRDD = sc.textFile("D:\\Work\\intellij-workspace\\ScalaSpark\\data\\retail_db\\orders\\orders.csv")
    val orderItemsRDD = sc.textFile("D:\\Work\\intellij-workspace\\ScalaSpark\\data\\retail_db\\order_items\\order_items.csv")

    // Apply filter to extract only CANCELED orders and the apply map function to get order_id as key and entire record as value from orders table.
    val ordersMap = ordersRDD.filter(rec => rec.split(",")(3).equals("CANCELED")).map(rec => (rec.split(",")(0).toInt, rec))

    // Apply map function to get order_item_order_id as key and order_item_subtotal as value from order_items table.
    val orderItemsMap = orderItemsRDD.map(rec => (rec.split(",")(1).toInt, rec.split(",")(4).toFloat))

    // Aggregate the subtotal per order
    val orderItemsAgg = orderItemsMap.reduceByKey((acc, value) => (acc + value))

    // Join data sets using spark transformation join. ordersJoinOrderItems will represent a tuple. Key is join column (order_id) value is a
    // tuple with corresponding orders record as first element and order_total as second element
    val ordersJoinOrderItems = ordersMap.join(orderItemsAgg)
    ordersJoinOrderItems.filter(rec => rec._2._2 >= 1000).sortByKey().map(rec => rec._2).collect().foreach(println)
  }

  /* Global Sorting */
  def globalSortingExamples(sc: SparkContext): Unit = {

    val productsRDD = sc.textFile("D:\\Work\\intellij-workspace\\ScalaSpark\\data\\retail_db\\products\\products.csv")
    val productsMap = productsRDD.map(rec => (rec.split(",")(4).toFloat, rec))

    // Get the products sorted by price from products table
    //productsMap.sortByKey().collect().foreach(println)

    // Get the top 5 highest priced products from products table. The below two statements are same
    //productsMap.sortByKey(false).take(5).foreach(println)
    //productsMap.top(5).foreach(println) // by default top sorts the data by Key

    // Using takeOrdered gives better flexibility, we can order by any field in the dataset, and we need not call .map before
    productsRDD.takeOrdered(5)(Ordering.Float.reverse.on(rec => rec.split(",")(4).toFloat)).foreach(println) // functionality of map + sortByKey
  }

  /* Global Sorting */
  def sortingByKeyExamples(sc: SparkContext): Unit = {

    val productsRDD = sc.textFile("C:\\Anindya\\Work\\intellij-workspace\\ScalaSpark\\data\\retail_db\\products\\products.csv")
    val productsMap = productsRDD.map(rec => (rec.split(",")(1).toInt, rec))
    val productsGroupBy = productsMap.groupByKey()

    // Using groupByKey creates a tuple with Key and a List of Values
    //productsGroupBy.collect().foreach(println)
    // I order to get that list of values in a more human readable format or as single record use flatMap
    //productsGroupBy.flatMap(rec => rec._2).collect().foreach(println)

    // Since after groupByKey the value is a iterable so we need to first convert it to a List to apply any sort function
    // if the value was a list of string we could have easily used list.sorted method but since each value in the entire list
    // is itself a big comma separated string we  need to use a sortBy function and parse the string to extract out the exact fields on which to sort
    //productsGroupBy.map(rec => (rec._1, rec._2.toList.sortBy(k => k.split(",")(4).toFloat))).collect().foreach(println)

    // if we use flatMap then we cannot generate tuple
    //productsGroupBy.flatMap(rec => rec._2.toList.sortBy(k => k.split(",")(4).toFloat)).collect().foreach(println)

    // to sort in descending order u can use - infront of the comparator for numeric fields
    //productsGroupBy.flatMap(rec => rec._2.toList.sortBy(k => -k.split(",")(4).toFloat)).collect().foreach(println)

    // using a function inside flatMap
    //productsGroupBy.flatMap(getAll(_).toList.sortBy(k => k.split(",")(4).toFloat)).collect().foreach(println)

    // using a function inside flatMap
    productsGroupBy.flatMap(sortList).collect().foreach(println)
  }

  /* Global Sorting */
  def rankingByKeyExamples(sc: SparkContext): Unit = {

    val productsRDD = sc.textFile("C:\\Anindya\\Work\\intellij-workspace\\ScalaSpark\\data\\retail_db\\products\\products.csv")
    val productsMap = productsRDD.map(rec => (rec.split(",")(1).toInt, rec))
    val productsGroupBy = productsMap.groupByKey()

    // For each product category get the top 5 records. i.e. top 5 products in each category (some of the products may have same price)
    //productsGroupBy.flatMap(getTopN(_, 5)).collect().foreach(println)

    // For each category get top 3 priced products i.e. if there are 10 products with top 3 prices, the RDD should give us all 10 products
    productsGroupBy.flatMap(getTopDenseN(_, 3)).collect().foreach(println)
  }

  // example function that  can be used in flatMap method
  def getAll(rec: (Int, Iterable[String])): Iterable[String] = {
    return rec._2
  }

  // example function that  can be used in flatMap method
  def sortList(rec: (Int, Iterable[String])): Iterable[String] = {
    return rec._2.toList.sortBy(k => k.split(",")(4).toFloat)
  }

  // To get topN products by price in each category
  def getTopN(rec: (Int, Iterable[String]), topN:Int): Iterable[String] = {
    return rec._2.toList.sortBy(k => k.split(",")(4).toFloat).take(topN)
  }

  // to get topN priced products by category
  def getTopDenseN(rec: (Int, Iterable[String]), topN:Int): Iterable[String] = {
    var prodPrices: List[Float] = List[Float]()
    var topNPrices: List[Float] = List[Float]()
    var sortedRecs: List[String] = List[String]()

    for(proddata <- rec._2) {
      prodPrices = prodPrices:+ proddata.split(",")(4).toFloat
    }

    topNPrices = prodPrices.sortBy(k => -k).take(topN)

    sortedRecs = rec._2.toList.sortBy(k => -k.split(",")(4).toFloat)

    var result: List[String] = List[String]()
    for(proddata <- sortedRecs) {
      if(topNPrices.contains(proddata.split(",")(4).toFloat))
        result = result:+ proddata
    }

    return result
  }
}
