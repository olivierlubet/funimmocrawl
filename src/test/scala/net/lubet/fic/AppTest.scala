package net.lubet.fic

import java.net.URL

import net.lubet.fic.lbc.ListPage
import org.scalatest.FunSuite
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.{Row, SparkSession}


class AppTest extends FunSuite {
  val warehouseLocation = "file:///Users/olivi/IdeaProjects/funimmocrawl/spark"
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val spark = SparkSession.builder.master("local").appName("Test").config("spark.sql.warehouse.dir", warehouseLocation).getOrCreate()

  test("pipeline") {
    val bu = BaseurlDF.load(spark)

    import spark.implicits._
    val buPages = bu.map { u: Row => ListPage.load(new URL(u.getString(0))).toHtml }
  }
}
