name := "funimmocrawl"

version := "0.1"

scalaVersion := "2.11.12"

// http://www.scalatest.org/
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"

// https://github.com/ruippeixotog/scala-scraper
libraryDependencies += "net.ruippeixotog" %% "scala-scraper" % "2.1.0"


val sparkVersion = "2.3.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "mysql" % "mysql-connector-java" % "5.1.6"
)