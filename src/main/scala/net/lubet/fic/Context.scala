package net.lubet.fic

import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Context {
  lazy val spark: SparkSession = {
    System.setSecurityManager(null)

    //val warehouseLocation = "file:///Users/olivi/IdeaProjects/funimmocrawl/spark"
    val warehouseLocation = "file://"+new File("spark").getAbsolutePath
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    SparkSession
      .builder
      .master("local[4]")
      .appName("FIC")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()
  }
}
