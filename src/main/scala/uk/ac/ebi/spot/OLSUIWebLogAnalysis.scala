package uk.ac.ebi.spot

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.{Dataset, RelationalGroupedDataset, SparkSession, functions}

class OLSUIWebLogAnalysis(sparkSession: SparkSession, logFilesToRead: String)
  extends AbstractOLSWebLogAnalysis(sparkSession, logFilesToRead) {

  def includeLogEntries(dataset: Dataset[LogFileFormat]): Dataset[LogFileFormat] = {
    val status200Dataset: Dataset[LogFileFormat] = dataset.filter(logFileLine => logFileLine.status == "200")
    logger.trace("Status 200: Number of lines = " + status200Dataset.count())
    status200Dataset.show(10, false)
    status200Dataset
  }

  def excludeLogEntries(dataset: Dataset[LogFileFormat]): Dataset[LogFileFormat] = {
    val withoutInternalCallsDataset: Dataset[LogFileFormat] = dataset.filter(logFileLine => {
      !logFileLine.request.contains("js") && !logFileLine.request.contains("img") &&
        !logFileLine.request.contains("css") && !logFileLine.request.contains("/api")
    })
    logger.trace("Without internal calls: Number of lines = " + withoutInternalCallsDataset.count())
    withoutInternalCallsDataset.show(10, false)
    withoutInternalCallsDataset
  }
}

object OLSUIWebLogAnalysis  {
  private val logger = Logger[OLSUIWebLogAnalysis]

//  def main(args: Array[String]):Unit = {
//    val logFilesToRead = args(0)
//    logger.trace("logFilesToRead = " + logFilesToRead)
//    val outputFile = args(1)
//    val sparkSession = SparkSession.builder().appName("OLSUIWebLogAnalysis").
//      master("local[*]").getOrCreate()
//
//    sparkSession.sparkContext.setLogLevel("ERROR")
//
//    val logAnalysis: OLSUIWebLogAnalysis = new OLSUIWebLogAnalysis(sparkSession, logFilesToRead)
//    val cleanedDataset =  logAnalysis.excludeLogEntries(logAnalysis.includeLogEntries(
//      logAnalysis.parseDataset(logAnalysis.readLogFiles())))
//
//    val groupedByDate = cleanedDataset.groupBy("year", "month").count()
//    groupedByDate.show(12, false)
////    groupedByDate.toJavaRDD.saveAsTextFile(outputFile)
//    sparkSession.stop()
//  }
}
