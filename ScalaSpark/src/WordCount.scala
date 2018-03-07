import java.io.IOException
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
//import com.typesafe.scalalogging.LazyLogging

import org.apache.spark.rdd.RDD

object WordCount {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\Work\\spark-1.6.1-bin-hadoop2.6")
    val conf = new SparkConf().setAppName("ClouderaRetailDb Exercises").setMaster("local[2]")
    val sc = new SparkContext(conf)

    //val infile = args {0}
    var infile = "D:\\Work\\intellij-workspace\\ScalaSpark\\data\\wordcount.txt"

    wordCount1(sc, infile)
    wordCount2(sc, infile)
    wordCount3(sc, infile)
    wordCount4(sc, infile)
    wordCount5(sc, infile)
    wordCount6(sc, infile)
    wordCount7(sc, infile)
    wordCount8(sc, infile, "D:\\Work\\intellij-workspace\\ScalaSpark\\output")

    sc.stop()
  }

  /* Read the file and print every line */
  def wordCount1(sc: SparkContext, infile: String): Unit = {

    // read the file
    val data = sc.textFile(infile)

    // Let's print each line of the book
    data.foreach(println)
  }

  /* Read the file and print every word */
  def wordCount2(sc: SparkContext, infile: String): Unit = {

    // read the file
    val data = sc.textFile(infile)

    // Let's print print each and every word
    data.flatMap(line => line.split("\\s"))
      .foreach(println)
  }

  /* Read the file, generate words, filter out "empty" word and print each word */
  def wordCount3(sc: SparkContext, infile: String): Unit = {

    // read the file
    val data = sc.textFile(infile)

    // Let's generate words, filter out "empty" word and print each word
    data.flatMap(line => line.split("\\s"))
      .filter(word => !word.isEmpty)
      .foreach(println)
  }

  /* Read the file, generate words, trim each word, put in lower case, replace special char, filter out "empty" word and print each word */
  def wordCount4(sc: SparkContext, infile: String): Unit = {

    // read the file
    val data = sc.textFile(infile)

    // Let's generate words, trim each word, put in lowercase, replace special char, filter out "empty" word and print each word
    val regex = "[,.:;'\"\\?\\-!\\(\\)]".r
    data.flatMap(line => line.split("\\s"))
      .map(word => regex.replaceAllIn(word.trim().toLowerCase(), ""))
      .filter(word => !word.isEmpty() && word.length > 2)
      .foreach(println)
  }

  /* Read the file, generate words, trim each word, put in lower case, replace special char,
   filter out "empty" word, count each word and print the word and the count */
  def wordCount5(sc: SparkContext, infile: String): Unit = {

    val data = sc.textFile(infile)

    // Let's generate words, trim each word, put in lowercase, replace special char, filter out "empty" word, count each word and print the word and the count
    val regex = "[,.:;'\"\\?\\-!\\(\\)]".r
    data.flatMap(line => line.split("\\s"))
      .map(word => regex.replaceAllIn(word.trim().toLowerCase(), ""))
      .filter(word => !word.isEmpty() && word.length > 2)
      .map(word => (word, 1))
      .reduceByKey((a, b) => a + b)
      .foreach(println)
  }

  /* Read the file, generate words, trim each word, put in lower case, replace special char,
    filter out "empty" word, count each word, sort by count ASC and print the word and the count */
  def wordCount6(sc: SparkContext, infile: String): Unit = {

    // read the file
    val data = sc.textFile(infile)

    // Let's generate words, trim each word, put in lowercase, replace special char, filter out "empty" word, count each word, sort by count ASC and print the word and the count
    val regex = "[,.:;'\"\\?\\-!\\(\\)]".r
    data.flatMap(line => line.split("\\s"))
      .map(word => regex.replaceAllIn(word.trim().toLowerCase(), ""))
      .filter(word => !word.isEmpty())
      .map(word => (word, 1))
      .reduceByKey((a, b) => a + b)
      .map(tuple => (tuple._2, tuple._1)).sortByKey(true)
      .foreach(println)
  }

  /*
   1. reading the file, generate words, replace special char, filter out "empty", count each word, sort by count ASC
   2. store the result in memory
   3. print the word and the count
   4. print the word only
   5. print the number of word that start with each char for only the word with more than 10 occurrences. The result is sorted by count ASC
  */
  def wordCount7(sc: SparkContext, infile: String): Unit = {

    // read the file
    val data = sc.textFile(infile)

    val regex = "[,.:;'\"\\?\\-!\\(\\)]".r
    val result = data.flatMap(line => line.split("\\s"))
                  .map(word => regex.replaceAllIn(word.trim().toLowerCase(), ""))
                  .filter(word => !word.isEmpty())
                  .map(word => (word, 1))
                  .reduceByKey((a,b) => a + b)
                  .map(tuple => (tuple._2, tuple._1)).sortByKey(true)
    result.persist(StorageLevel.MEMORY_ONLY)
    result.foreach(println)

    result.map(tuple => tuple._2).foreach(println)

    result.filter(tuple => tuple._1 > 10)
        .map(tuple => (tuple._2.charAt(0), tuple._1))
        .reduceByKey((a, b) => a + b)
        .map(tuple => (tuple._2, tuple._1)).sortByKey(true)
        .foreach(println)
  }

  /*
   1. reading the file, generate words, count each word
   2. store the word splits in a file
   3. store the word counts in a file
  */
  def wordCount8(sc: SparkContext, infile: String, outdir: String): Unit = {

    val tf = sc.textFile(infile, 2).cache()
    val splits = tf.flatMap(line => line.split(" ")).map(word => (word, 1))
    val counts = splits.reduceByKey((a, b) => a + b)

    deleteDir(outdir + "\\SplitOutput")
    deleteDir(outdir + "\\CountOutput")

    splits.saveAsTextFile(outdir + "\\SplitOutput")
    counts.saveAsTextFile(outdir + "\\CountOutput")
  }

  def deleteDir(dirName: String): Unit = {
    val path = FileSystems.getDefault.getPath(dirName)
    if (Files.exists(path) && Files.isDirectory(path)) {
      Files.walkFileTree(path, new FileVisitor [Path] {
        def visitFileFailed(file: Path, exc: IOException) = FileVisitResult.CONTINUE

        def visitFile(file: Path, attrs: BasicFileAttributes) = {
          Files.delete(file)
          FileVisitResult.CONTINUE
        }

        def preVisitDirectory(dir: Path, attrs: BasicFileAttributes) = FileVisitResult.CONTINUE

        def postVisitDirectory(dir: Path, exc: IOException) = {
          Files.delete(dir)
          FileVisitResult.CONTINUE
        }
      })
    }
  }
}
