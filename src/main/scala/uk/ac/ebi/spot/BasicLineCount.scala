package uk.ac.ebi.spot

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class BasicLineCount(private val sparkSession: SparkSession, private val logFilesToRead: String)
  extends WebLogAnalysis [String] {

  def readLogFiles(): Dataset[Row] = {
    val baseDataFrame = sparkSession.read.text(logFilesToRead)

    baseDataFrame
  }

  override def parseDataset(dataset: Dataset[Row]): Dataset[String] = ???

  override def includeLogEntries(dataset: Dataset[String]): Dataset[String] = ???

  override def excludeLogEntries(dataset: Dataset[String]): Dataset[String] = ???
}

object BasicLineCount {
    val logger = Logger[BasicLineCount]

    def main(args: Array[String]): Unit = {
      val logFilesToRead = args(0)
      logger.trace("logFilesToRead = " + logFilesToRead)

      val sparkSession = SparkSession.builder()
        //      .appName("DefaultLogAnalysis")
        //      .config("spark.executor.memory", "2g")
        .getOrCreate()
      //    sparkSession.sparkContext.setLogLevel("TRACE")

      val lineCount: BasicLineCount = new BasicLineCount(sparkSession, logFilesToRead)
      val logFileDataset: Dataset[Row] = lineCount.readLogFiles()
      logger.trace("Number of lines = " + logFileDataset.count())
    }
}