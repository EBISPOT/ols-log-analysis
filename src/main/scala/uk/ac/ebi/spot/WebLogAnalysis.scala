package uk.ac.ebi.spot

import org.apache.spark.sql.{Dataset, Row}

trait WebLogAnalysis[T] extends Serializable {
  def readLogFiles(): Dataset[Row]
  def parseDataset(dataset: Dataset[Row]): Dataset[T]
  def includeLogEntries(dataset: Dataset[T]): Dataset[T]
  def excludeLogEntries(dataset: Dataset[T]):Dataset[T]
}

