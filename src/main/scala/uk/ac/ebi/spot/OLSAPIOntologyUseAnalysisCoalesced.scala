package uk.ac.ebi.spot

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{month, regexp_extract, to_date, year}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row, SparkSession}


object OLSAPIOntologyUseAnalysisCoalesced {
  private val logger = Logger[OLSUIWebLogAnalysis]

//  def main(args: Array[String]): Unit = {
//    val serverStatistics = args(0)
//    logger.trace("logFilesToRead = " + serverStatistics)
//    val outputFile = args(1)
//
//    val sparkSession = SparkSession.builder()
//      .appName("OLSAPIOntologyUseAnalysisCoalesced")
//      .master("spark://localhost:7077")
//      .config("spark.jars", "target/scala-2.12/ols-log-analysis.jar")
//      .getOrCreate()
//    sparkSession.sparkContext.setLogLevel("ERROR")
//    val caseClassschema = Encoders.product[OntologyUseStatistics].schema
//    val apiOntologyUseDataset = sparkSession.read
//      .option("header", "true")
//      .option("delimiter", ",")
//      .schema(caseClassschema)
//      .csv(serverStatistics)
//    apiOntologyUseDataset.show(10)
//
//    val apiOntologyUseDatasetSum = apiOntologyUseDataset.groupBy("year", "month", "ontology")
//      .sum("count").as("requests")
//    apiOntologyUseDatasetSum.show()
//
//    val windowSpec  = Window.partitionBy("month").orderBy("request")
//
//    val apiOntologyUseDatasetTop10Percent = apiOntologyUseDatasetSum.withColumn("percent_rank",
//      percent_rank().over(windowSpec))
//
//    apiOntologyUseDatasetTop10Percent.show()
//
////
////    import sparkSession.implicits._
////    val apiOnotologyUseDatasetSorted = apiOntologyUseDatasetSum.sort($"month", $"requests".desc)
////
////
////
////
////
////    apiOntologyUseDatasetSum.coalesce(1)
////      .write
////      .option("header","true")
////      .option("sep",",")
////      .mode("overwrite")
////      .csv(outputFile)
//    sparkSession.stop()
//  }
}
case class OntologyUseStatistics(year: Integer, month: Integer, ontology: String, count: Integer)


