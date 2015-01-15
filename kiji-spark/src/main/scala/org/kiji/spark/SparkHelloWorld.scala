package org.kiji.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SparkHelloWorld {
  /**
   * Prints a Hello World message and details of the SparkConf object.
   *
   * @param args passed in from the command line; expects one argument
   *     to be specified that is a path to the file.
   */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("HelloWorld")
    println("Hello World!")
    println(conf)

    assert(args.length == 1, "Requires one argument: the path to a text file.")

    val context = new SparkContext("local", "Local Test Launch")
    val lines = context.textFile(args(0))
    val lineCount = lines.count()

    val linesWithSpark = lines.filter({ line => line.contains("Spark") })
    val linesWithSparkCount = linesWithSpark.count()
    println(s"There are $linesWithSparkCount lines in the file that contain the word \'Spark\'.")
  }
}