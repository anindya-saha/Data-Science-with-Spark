import org.apache.spark.{SparkConf, SparkContext}

// Import Spark SQL data types
import org.apache.spark.sql._

// Import mllib recommendation data types
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}


// input format of movies.dat MovieID::Title::Genres
// 1::Toy Story (1995)::Animation|Children's|Comedy
// 2::Jumanji (1995)::Adventure|Children's|Fantasy
case class Movie(movieId: Int, title: String, genres: Seq[String])

// input format of users.dat is UserID::Gender::Age::Occupation::Zip-code
// 1::F::1::10::48067
// 2::M::56::16::70072
case class User(userId: Int, gender: String, age: Int, occupation: Int, zip: String)

object MovieRecommender {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\Work\\spark-1.6.1-bin-hadoop2.6")
    val conf = new SparkConf().setAppName("Movie Recommender").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // SQLContext entry point for working with structured data
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // function to parse input into Movie class
    def parseMovie(str: String): Movie = {
      val fields: Array[String] = str.split("::")
      assert(fields.size == 3)
      Movie(fields(0).toInt, fields(1), Seq(fields(2)))
    }

    // function to parse input into User class
    def parseUser(str: String): User = {
      val fields: Array[String] = str.split("::")
      assert(fields.size == 5)
      User(fields(0).toInt, fields(1).toString, fields(2).toInt, fields(3).toInt, fields(4).toString)
    }

    // load the ratings data into a RDD
    val ratingText = sc.textFile("D:\\Work\\intellij-workspace\\ScalaSpark\\data\\spark-ebook\\ratings.dat")

    // Return the first element in this RDD
    println(ratingText.first())

    // input format of ratings.dat is UserID::MovieId::Rating::Time
    // function to parse input UserID::MovieID::Rating/ Into org.apache.spark.mllib.recommendation.Rating class
    def parseRating(str: String): Rating = {
      val fields: Array[String] = str.split("::")
      //assert(fields.size == 5)
      Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
    }

    // create an RDD of Ratings objects
    // We cache the ratings data, since we will use this data to build the matrix model.
    import org.apache.spark.rdd.RDD
    val ratingsRDD: RDD[Rating] = ratingText.map(parseRating).cache()

    println("Total number of ratings: " + ratingsRDD.count())

    println("Total number of movies rated: " + ratingsRDD.map(rating => rating.product).distinct().count())

    println("Total number of users who rated movies: " + ratingsRDD.map(rating => rating.user).distinct().count())

    // This is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._

    // load the data from the users and movies data files into an RDD, use the map() transformation with the parse functions, and then call toDF() to create a DataFrame for the RDD.
    val usersDF = sc.textFile("D:\\Work\\intellij-workspace\\ScalaSpark\\data\\spark-ebook\\users.dat").map(parseUser).toDF()
    val moviesDF = sc.textFile("D:\\Work\\intellij-workspace\\ScalaSpark\\data\\spark-ebook\\movies.dat").map(parseMovie).toDF()

    // create a DataFrame from the ratingsRDD
    val ratingsDF = ratingsRDD.toDF()

    // Register the DataFrames as temp tables so that we can use the tables in SQL statements.
    ratingsDF.registerTempTable("ratings")
    moviesDF.registerTempTable("movies")
    usersDF.registerTempTable("users")

    // DataFrame printSchema() prints the schema to the console in a tree format

    usersDF.printSchema()

    moviesDF.printSchema()

    ratingsDF.printSchema()

    // Get the max, min ratings along with the count of users who have rated a movie.
    sqlContext.sql("set spark.sql.shuffle.partitions=10")
    val results = sqlContext.sql(
                  """SELECT movies.title, movierates.maxr, movierates.minr, movierates.cntu
                    |FROM (
                    |       SELECT ratings.product, max(ratings.rating) as maxr, min(ratings.rating) as minr, COUNT(DISTINCT ratings.user) as cntu
                    |       FROM ratings
                    |       GROUP BY ratings.product
                    |      ) movierates
                    |JOIN movies ON movierates.product = movies.movieId
                    |ORDER BY movierates.cntu DESC
                  """.stripMargin)
    // DataFrame show() displays the top 20 rows in  tabular form
    results.show(20)

    // Show the top 10 most-active users and how many times they rated a movie
    val mostActiveUsersSchemaRDD = sqlContext.sql(
                                    """SELECT ratings.user, count(*) as cntrt from ratings
                                      |GROUP BY ratings.user
                                      |ORDER BY  cntrt DESC limit 10
                                    """.stripMargin)

    //println(mostActiveUsersSchemaRDD.collect().mkString("\n"))
    println("\nTop 10 Most Active Users...")
    mostActiveUsersSchemaRDD.collect().foreach(println)

    // Find the movies that the most active user 4169 rated higher than 4
    val moviesRatedOver4ByMostActiveUser = sqlContext.sql(
                  """SELECT ratings.user, ratings.product, ratings.rating, movies.title
                    |FROM ratings JOIN movies ON movies.movieId = ratings.product
                    |WHERE ratings.user = 4169 and ratings.rating > 4
                  """.stripMargin)

    println("\nMovies Rated over 4.0 by the Most Active User: 4169...")
    moviesRatedOver4ByMostActiveUser.show(20)

    // Randomly split ratings RDD into training data RDD (80%) and test data RDD (20%)
    val splits = ratingsRDD.randomSplit(Array(0.8, 0.2), 0L)

    val trainingRatingsRDD = splits(0).cache()
    val testingRatingsRDD = splits(1).cache()

    val numTraining = trainingRatingsRDD.count()
    val numTest = testingRatingsRDD.count()
    println(s"Training: $numTraining, test: $numTest")

    /* MODEL TRAINING
     * ==============
     * We run ALS on the input trainingRDD of Rating(user, product, rating) objects with the rank and Iterations parameters:
     * 1. rank is the number of latent factors in the model.
     * 2. iterations is the number of iterations to run.
     * The ALS run(trainingRDD) method will build and return a MatrixFactorizationModel, which can be used to make product predictions for users.
     */
    // build a ALS user product matrix model with rank=20, iterations=10
    val model: MatrixFactorizationModel = new ALS().setRank(20).setIterations(10).run(trainingRatingsRDD)

    /*
     * MAKING PREDICTIONS
     * ==================
     * Now we can use the MatrixFactorizationModel to make predictions.
     * First, we will get movie predictions for the most active user, 4169, with the recommendProducts() method, which takes as input the userid and the number of products to recommend.
     * Then we print out the recommended movie titles.
     */
    // Get the top 4 movie predictions for user 4169
    val topRecsForUser: Array[Rating] = model.recommendProducts(4169, 5)

    // get movie titles to show with recommendations
    val movieTitles = moviesDF.map(rec => (rec(0), rec(1))).collectAsMap()

    // print out top recommendations for user 4169 with titles
    println("\nTop recommendations for user 4169 with titles...")
    topRecsForUser.map(rating => (movieTitles(rating.product))).foreach(println)

    /*
     * MODEL EVALUATION
     * ================
     * Now, we will compare predictions from the model with actual ratings in the testRatingsRDD.
     * First we get the user product pairs from the testRatingsRDD to pass to the MatrixFactorizationModel predict(user: Int, product: Int) method,
     * which will return predictions as Rating(user, product, rating) objects.
     */
    // get user product pair from testRatings
    val testUserProductRDD = testingRatingsRDD.map{case Rating(user, product, rating) => (user, product)}

    // get predicted ratings to compare to test ratings
    val predictedRatingsForTestRDD = model.predict(testUserProductRDD)

    println("\nSample predictions by the model on the test data...")
    predictedRatingsForTestRDD.sample(false, 0.0002).foreach(println)

    /*
     * Now we will compare the test predictions to the actual test ratings.
     * First we put the predictions and the test RDDs in this key, value pair format for joining: ((user, product), rating).
     * Then we print out the (user, product), (test rating, predicted rating) for comparison.
     */
    // prepare predictions for comparison
    val predictedTestRatingsKeyedByUserProductRDD = predictedRatingsForTestRDD.map{case Rating(user, product, rating) => ((user, product), rating)}

    // prepare test for comparison
    val actualTestRatingsKeyedByUserProductRDD = testingRatingsRDD.map{case Rating(user, product, rating) => ((user, product), rating)}

    //Join the Actual ratings and Predicted ratings on the test data
    val actualAndPredictedRatingsJoinedRDD = actualTestRatingsKeyedByUserProductRDD.join(predictedTestRatingsKeyedByUserProductRDD)

    println("\nActual and Predicted Ratings Comparison Side By Side...")
    actualAndPredictedRatingsJoinedRDD.take(10).foreach(println)

    val falsePositives = actualAndPredictedRatingsJoinedRDD.filter{case((user, product), (trueRating, predictedRating)) => (trueRating <= 1 && predictedRating >= 4)}
    println("\nFalse Positives...")
    falsePositives.take(2).foreach(println)

    println(f"\nFalse Positives Count = ${falsePositives.count()}%d")

    // Evaluate the model using Mean Absolute Error (MAE) between test and predictions
    val MeanAbsoluteError = actualAndPredictedRatingsJoinedRDD.map{case((user, product), (trueRating, predictedRating)) => Math.abs(trueRating - predictedRating)}.mean()
    println(f"MEAN ABSOLUTE ERROR = $MeanAbsoluteError%f")
  }
}
