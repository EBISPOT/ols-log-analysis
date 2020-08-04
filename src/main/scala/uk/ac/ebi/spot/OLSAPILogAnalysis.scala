package uk.ac.ebi.spot

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.{Dataset, SparkSession}

class OLSAPILogAnalysis(sparkSession: SparkSession, logFilesToRead: String)
  extends AbstractOLSWebLogAnalysis(sparkSession, logFilesToRead) {

  def includeLogEntries(dataset: Dataset[LogFileFormat]): Dataset[LogFileFormat] = {
    val status200Dataset: Dataset[LogFileFormat] = dataset.filter(logFileLine =>
      logFileLine.status == "200" && logFileLine.request.contains("/api"))
    logger.trace("Status 200: Number of lines = " + status200Dataset.count())
    status200Dataset.show(10, false)
    status200Dataset
  }
  def excludeLogEntries(dataset: Dataset[LogFileFormat]): Dataset[LogFileFormat] = {
    val withoutInternalCallsDataset: Dataset[LogFileFormat] = dataset.filter(logFileLine => {
      !logFileLine.request.contains("js") && !logFileLine.request.contains("img") &&
        !logFileLine.request.contains("css")
    })
    logger.trace("Without internal calls: Number of lines = " + withoutInternalCallsDataset.count())
    withoutInternalCallsDataset.show(10, false)
    withoutInternalCallsDataset
  }
}

object OLSAPILogAnalysis {
  private val logger = Logger[OLSAPILogAnalysis]

  def main(args: Array[String]): Unit = {
    val logFilesToRead = args(0)
    logger.trace("logFilesToRead = " + logFilesToRead)
    val outputFile = args(1)

    val sparkSession = SparkSession.builder().appName("OLSAPILogAnalysis").master("local[*]")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")

    val logAnalysis: OLSAPILogAnalysis = new OLSAPILogAnalysis(sparkSession, logFilesToRead)
    val cleanedDataset =  logAnalysis.excludeLogEntries(logAnalysis.includeLogEntries(
      logAnalysis.parseDataset(logAnalysis.readLogFiles())))

    logger.trace("Cleaned data: Number of lines = " + cleanedDataset.count())
//    cleanedDataset.toJavaRDD.saveAsTextFile(outputFile)
    val groupedByDate = cleanedDataset.groupBy("month").count()
     groupedByDate.show(12, false)

    sparkSession.stop()
  }
}

