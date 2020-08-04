package uk.ac.ebi.spot

import java.time.LocalDateTime

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.functions.{date_format, regexp_extract, to_date, month, year}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

abstract class AbstractOLSWebLogAnalysis(private val sparkSession: SparkSession, private val logFilesToRead: String)
//  extends AbstractWebLogAnalysis(sparkSession, logFilesToRead) {
  extends WebLogAnalysis [LogFileFormat] {

  val logger = Logger[AbstractOLSWebLogAnalysis]

  def readLogFiles(): Dataset[Row] = {
    val baseDataFrame = sparkSession.read.text(logFilesToRead)
    logger.trace("Number of logfile lines = " + baseDataFrame.count())
    val withoutBotsDataFrame = baseDataFrame.filter(l =>
      !l.getString(0).contains("bot") && !l.getString(0).contains("crawler") &&
        !l.getString(0).contains("root") && !l.getString(0).contains("graph"))
    logger.trace("Number of logfile lines without bots = " + withoutBotsDataFrame.count())

    withoutBotsDataFrame.show(10, false)
    withoutBotsDataFrame
  }

  def parseDataset(dataset: Dataset[Row]): Dataset[LogFileFormat] = {
    import sparkSession.implicits._
    sparkSession.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    // See https://stackoverflow.com/questions/7015715/a-regex-pattern-for-different-tomcats-log-entries/7073775
    val regEx: String = "^(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3} )?(\\S+) (\\S+) \\[(\\d\\d/\\w{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2})\\s\\S\\d{4}\\] \"(.*?)\" (\\S+) (\\S+)( \"(.*?)\" \"(.*?)\")?"
    val parsedDataFrame = dataset.select(
      regexp_extract($"value", regEx, 1).as("remoteIPAddress"),
      regexp_extract($"value", regEx, 4).as("dateTime"),
      month(to_date(regexp_extract($"value", regEx, 4), "dd/MMM/yyyy")).as("month"),
      year(to_date(regexp_extract($"value", regEx, 4), "dd/MMM/yyyy")).as("year"),
      regexp_extract($"value", regEx, 5).as("request"),
      regexp_extract($"value", regEx, 6).as("status"),
      regexp_extract($"value", regEx, 7).as("bytes"),
      regexp_extract($"value", regEx, 9).as("referer"),
      regexp_extract($"value", regEx, 10).as("userAgent")
    )
    parsedDataFrame.as[LogFileFormat]
  }
}

//'%a %l %u %t "%r" %s %b "%{Referer}i" "%{User-agent}i"'
//%a - Remote IP address
//%l - Remote logical username from identd (always returns '-')
//%u - Remote user that was authenticated (if any), else '-'
//%t - Date and time, in Common Log Format
//%r - First line of the request (method and request URI)
//%s - HTTP status code of the response
//%b - Bytes sent, excluding HTTP headers, or '-' if zero
//%{xxx}i write value of incoming header with name xxx
//import java.sql.Timestamp
case class LogFileFormat(remoteIPAddress: String, dateTime: String, month: Int, year: Int, request: String,
                         status: String, bytes: String, referer: String, userAgent: String)

