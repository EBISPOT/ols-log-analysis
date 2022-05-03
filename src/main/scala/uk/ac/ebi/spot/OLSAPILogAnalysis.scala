package uk.ac.ebi.spot

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.{Dataset, SparkSession}

class OLSAPILogAnalysis(sparkSession: SparkSession, logFilesToRead: String)
  extends AbstractOLSWebLogAnalysis(sparkSession, logFilesToRead) {

  def includeLogEntries(dataset: Dataset[LogFileFormat]): Dataset[LogFileFormat] = {
    val status200Dataset: Dataset[LogFileFormat] = dataset.filter(logFileLine =>
//      logFileLine.status == "200" && logFileLine.request.contains("/api")
      logFileLine.status.get.contains("200") && logFileLine.request.get.contains("/api")
    )
//    logger.trace("Status 200: Number of lines = " + status200Dataset.count())
//    status200Dataset.show(10, false)
    status200Dataset
  }
  def excludeLogEntries(dataset: Dataset[LogFileFormat]): Dataset[LogFileFormat] = {
    val withoutInternalCallsDataset: Dataset[LogFileFormat] = dataset.filter(logFileLine => {
      logFileLine.request.isDefined && logFileLine.status.isDefined && logFileLine.month.isDefined &&
      !logFileLine.request.get.contains("js") && !logFileLine.request.get.contains("img") &&
        !logFileLine.request.get.contains("css")
    })
//    logger.trace("Without internal calls: Number of lines = " + withoutInternalCallsDataset.count())
//    withoutInternalCallsDataset.show(10, false)
    withoutInternalCallsDataset
  }
}

object OLSAPILogAnalysis {
  private val logger = Logger[OLSAPILogAnalysis]

  def main(args: Array[String]): Unit = {
//    System.setProperty("log4j.configurationFile", "log4j2.xml")
    val logFilesToRead = args(0)
    logger.trace("logFilesToRead = " + logFilesToRead)
//    val outputFile = args(1)

    val sparkSession = SparkSession.builder()
      .appName("OLSAPILogAnalysis")
      .master("local[*]")
      .config("spark.executor.memory", "2g")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")

    val logAnalysis: OLSAPILogAnalysis = new OLSAPILogAnalysis(sparkSession, logFilesToRead)
    val logFileDataset: Dataset[LogFileFormat] = logAnalysis.parseDataset(logAnalysis.readLogFiles())
//    logFileDataset.show(10)
    val cleanedDataset = logAnalysis.excludeLogEntries(logFileDataset)
    cleanedDataset.show(10)

//    val cleanedDataset =  logAnalysis.excludeLogEntries(logAnalysis.includeLogEntries(
//      logAnalysis.parseDataset(logAnalysis.readLogFiles())))

   logger.trace("Cleaned data: Number of lines = " + cleanedDataset.count())
//    cleanedDataset.toJavaRDD.saveAsTextFile(outputFile)
//    val groupedByDate = cleanedDataset.groupBy("month").count()
//     groupedByDate.show(24, false)

    sparkSession.stop()
  }
}

