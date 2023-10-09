package streaming

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream

object WordFrequency {

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      System.err.println("Usage: WordFrequency <HDFS_INPUT_PATH> <HDFS_OUTPUT_PATH>")
      System.exit(1)
    }

    val hdfsInputPath = args(0)
    val hdfsOutputPath = args(1)

    val sparkConf = new SparkConf().setAppName("WordFrequency")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // Task A
    processTaskA(ssc, hdfsInputPath, hdfsOutputPath)

    // Task B
    processTaskB(ssc, hdfsInputPath, hdfsOutputPath)

    // Task C
    processTaskC(ssc, hdfsInputPath, hdfsOutputPath)

    ssc.start()
    ssc.awaitTermination()
  }

  def createDStream(ssc: StreamingContext, hdfsInputPath: String): DStream[String] = {
    ssc.textFileStream(hdfsInputPath)
  }

  def processTaskA(ssc: StreamingContext, hdfsInputPath: String, hdfsOutputPath: String): Unit = {
    val inputDStream: DStream[String] = createDStream(ssc, hdfsInputPath)
    val processedDStream: DStream[String] = processDStreamTaskA(inputDStream)
    saveToHDFS(processedDStream, hdfsOutputPath + "/taskA")
  }

  def processTaskB(ssc: StreamingContext, hdfsInputPath: String, hdfsOutputPath: String): Unit = {
    val inputDStream: DStream[String] = createDStream(ssc, hdfsInputPath)
    val processedDStream: DStream[String] = processDStreamTaskB(inputDStream)
    saveToHDFS(processedDStream, hdfsOutputPath + "/taskB")
  }

  def processTaskC(ssc: StreamingContext, hdfsInputPath: String, hdfsOutputPath: String): Unit = {
    val inputDStream: DStream[String] = createDStream(ssc, hdfsInputPath)
    val processedDStream: DStream[String] = processDStreamTaskC(inputDStream)
    saveToHDFS(processedDStream, hdfsOutputPath + "/taskC")
  }

  def processDStreamTaskA(inputDStream: DStream[String]): DStream[String] = {
    // Task A logic
    inputDStream
      .flatMap(_.split(" "))
      .filter(word => word.forall(_.isLetter) && word.length >= 3)
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .map { case (word, count) => s"$word $count" }
  }

  def processDStreamTaskB(inputDStream: DStream[String]): DStream[String] = {
    val coOccurrences = inputDStream
      .window(Seconds(9), Seconds(3))
      .flatMap(line => {
        val words = line.split(" ").filter(word => word.forall(_.isLetter) && word.length >= 3)
        words.combinations(2).map(pair => (pair.mkString(" "), 1))
      })
      .reduceByKey(_ + _)
      .map { case (pair, count) => s"$pair $count" }

    coOccurrences
  }

  def processDStreamTaskC(inputDStream: DStream[String]): DStream[String] = {
    // Task C logic
    val updateFunction = (newValues: Seq[Int], runningCount: Option[Int]) =>
      Some(runningCount.getOrElse(0) + newValues.sum)

    inputDStream
      .flatMap(_.split(" "))
      .filter(word => word.forall(_.isLetter) && word.length >= 3)
      .map(word => (word, 1))
      .updateStateByKey(updateFunction)
      .map { case (word, count) => s"$word $count" }
  }

  def saveToHDFS(dstream: DStream[String], outputPath: String): Unit = {
    dstream.foreachRDD { (rdd, time) =>
      if (!rdd.isEmpty()) {
        rdd.saveAsTextFile(s"$outputPath-${time.milliseconds}")
      }
    }
  }
}
