ThisBuild / scalaVersion := "2.12.11"
ThisBuild / organization := "uk.ac.ebi.spot"

val setLog4jDebug = sys.props("log4j2.debug") = "true"

lazy val root = (project in file("."))
  .settings(
    name := "log-analysis",
	  libraryDependencies ++= Seq(
	    "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
      "org.slf4j" % "slf4j-api" % "1.7.30",
      "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.13.3",
      "org.apache.spark" %% "spark-core" % "3.0.0",
      "org.apache.spark" %% "spark-sql" % "3.0.0"
	  ),
    assemblyJarName in assembly := "ols-log-analysis.jar",
    scalacOptions ++= Seq("-deprecation")
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}


