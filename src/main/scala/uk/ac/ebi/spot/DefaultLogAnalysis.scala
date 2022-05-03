package uk.ac.ebi.spot

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import uk.ac.ebi.spot.DefaultLogAnalysis.logger


class DefaultLogAnalysis (private val sparkSession: SparkSession, private val logFilesToRead: String)
    extends WebLogAnalysis [String] {

  def readLogFiles(): Dataset[Row] = {
    val baseDataFrame = sparkSession.read.text(logFilesToRead)
    logger.trace("Number of logfile lines = " + baseDataFrame.count())
    val withoutBotsDataFrame = baseDataFrame.filter(l =>
      !l.getString(0).contains("bot") && !l.getString(0).contains("crawler"))
    logger.trace("Number of logfile lines without bots = " + withoutBotsDataFrame.count())

    withoutBotsDataFrame.show(10, false)
    withoutBotsDataFrame
  }

  override def parseDataset(dataset: Dataset[Row]): Dataset[String] = ???

  override def includeLogEntries(dataset: Dataset[String]): Dataset[String] = ???

  override def excludeLogEntries(dataset: Dataset[String]): Dataset[String] = ???
}

object DefaultLogAnalysis {
  val logger = Logger[DefaultLogAnalysis]

  def main(args: Array[String]): Unit = {
    val logFilesToRead = args(0)
    logger.trace("logFilesToRead = " + logFilesToRead)

    val sparkSession = SparkSession.builder()
//      .appName("DefaultLogAnalysis")
//      .config("spark.executor.memory", "2g")
      .getOrCreate()
//    sparkSession.sparkContext.setLogLevel("TRACE")

    val logAnalysis: DefaultLogAnalysis = new DefaultLogAnalysis(sparkSession, logFilesToRead)
    val logFileDataset: Dataset[Row] = logAnalysis.readLogFiles()
    logger.trace("Number of lines = " + logFileDataset.count())
  }
}
