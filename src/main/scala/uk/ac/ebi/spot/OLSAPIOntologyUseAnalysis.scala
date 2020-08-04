package uk.ac.ebi.spot

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 *
 */
class OLSAPIOntologyUseAnalysis(sparkSession: SparkSession, logFilesToRead: String)
  extends AbstractOLSOntologyUseAnalysis(sparkSession, logFilesToRead) {
  //  private val logger = Logger[OLSWebLogAnalysis]

  def includeLogEntries(dataset: Dataset[OntologyUseLogFormat]): Dataset[OntologyUseLogFormat] = {
    val status200Dataset: Dataset[OntologyUseLogFormat] = dataset.filter(logFileLine => logFileLine.status == "200")
    val ontologiesToInclude: Dataset[OntologyUseLogFormat] = status200Dataset.filter(logFileLine =>
      isAnOntologyToInclude(logFileLine.ontology))
    println("#################  includeLogEntries")
    logger.trace("Status 200: Number of lines = " + ontologiesToInclude.count())
    ontologiesToInclude.show(10, false)
    ontologiesToInclude
  }

  def excludeLogEntries(dataset: Dataset[OntologyUseLogFormat]): Dataset[OntologyUseLogFormat] = {
    val withoutInternalCallsDataset: Dataset[OntologyUseLogFormat] = dataset.filter(logFileLine => {
      !logFileLine.request.contains("js") && !logFileLine.request.contains("img") &&
        !logFileLine.request.contains("css") && !logFileLine.request.contains("/graph")
    })
    println("#################  excludeLogEntries")
    logger.trace("Without internal calls: Number of lines = " + withoutInternalCallsDataset.count())
    withoutInternalCallsDataset.show(10, false)
    withoutInternalCallsDataset
  }
}

object OLSAPIOntologyUseAnalysis {
  private val logger = Logger[OLSAPIOntologyUseAnalysis]

  def main(args: Array[String]): Unit = {
    val logFilesToRead = args(0)
    logger.trace("logFilesToRead = " + logFilesToRead)
    val outputFile = args(1)

    val sparkSession = SparkSession.builder().appName("OLSAPIOntologyUseAnalysis")
      .master("spark://localhost:7077")
      .config("spark.jars", "target/scala-2.12/ols-log-analysis.jar")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")

    val logAnalysis: OLSAPIOntologyUseAnalysis = new OLSAPIOntologyUseAnalysis(sparkSession, logFilesToRead)
    val cleanedDataset = logAnalysis.excludeLogEntries(logAnalysis.includeLogEntries(
      logAnalysis.parseDataset(logAnalysis.readLogFiles())))

    logger.trace("Cleaned data: Number of lines = " + cleanedDataset.count())
    val groupedByDate = cleanedDataset.groupBy("year", "month", "ontology").count()
    groupedByDate.show(Int.MaxValue, false)
    groupedByDate.coalesce(1)
      .write
      .option("header","true")
      .option("sep",",")
      .mode("overwrite")
      .csv(outputFile)

    sparkSession.stop()
  }
}

