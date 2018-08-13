package net.lubet.fic

import java.net.URL

import net.lubet.fic.lbc.ListPage
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Row, SparkSession}


object App extends App {
  println("FunImmoCrawl")

  val warehouseLocation = "file:///Users/olivi/IdeaProjects/funimmocrawl/spark"
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val spark = SparkSession.builder.master("local").appName("Test").config("spark.sql.warehouse.dir", warehouseLocation).getOrCreate()


  import spark.implicits._

  val listUrls: Dataset[String] = BaseurlDF.load(spark).flatMap { urlR: Row =>
    val urlS = urlR.getString(0)
    val l = ListPage.load(new URL(urlS))
    val nbPages = (l.getTotalAnnouncesCount / l.getAnnouncesUrl.size) + 1
    (1 to nbPages).map(urlS + "p-" + _)
  }

  listUrls.take(5).foreach(println)

  val detailsUrls = listUrls.flatMap { url: String => ListPage.load(new URL(url)).getAnnouncesUrl }

  detailsUrls.take(5).foreach(println)
}

