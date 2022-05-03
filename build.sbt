ThisBuild / scalaVersion := "2.12.15"
ThisBuild / organization := "uk.ac.ebi.spot"

val setLog4jDebug = sys.props("log4j2.debug") = "true "

lazy val root = (project in file("."))
  .settings(
    name := "ols-log-analysis",
	  libraryDependencies ++= Seq(
	    "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4",
      "org.slf4j" % "slf4j-api" % "1.7.33",
      "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.17.1",
      "org.apache.spark" %% "spark-core" % "3.1.2" % "provided",
      "org.apache.spark" %% "spark-sql" % "3.1.2" % "provided"
	  ),
    assemblyJarName in assembly := "ols-log-analysis.jar",
    scalacOptions ++= Seq("-deprecation"),
    excludeDependencies ++= Seq("org.slf4j" % "slf4j-log4j12")
)
