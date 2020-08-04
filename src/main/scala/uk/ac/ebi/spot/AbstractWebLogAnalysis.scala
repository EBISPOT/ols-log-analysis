package uk.ac.ebi.spot

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.{Dataset, Row, SparkSession}

abstract class AbstractWebLogAnalysis [T] (private val sparkSession: SparkSession, private val logFilesToRead: String)
  extends WebLogAnalysis [T] {
  val logger = Logger[AbstractWebLogAnalysis[T]]

  def readLogFiles(): Dataset[Row] = {
    val baseDataFrame = sparkSession.read.text(logFilesToRead)
    logger.trace("Number of logfile lines = " + baseDataFrame.count())
    val withoutBotsDataFrame = baseDataFrame.filter(l =>
      !l.getString(0).contains("bot") && !l.getString(0).contains("crawler"))
    logger.trace("Number of logfile lines without bots = " + withoutBotsDataFrame.count())

    withoutBotsDataFrame.show(10, false)
    withoutBotsDataFrame
  }
}
