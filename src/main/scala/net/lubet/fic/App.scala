package net.lubet.fic

import java.net.URL

import net.lubet.fic.lbc.{AnnouncePage, ListPage}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object App extends App {
  println("FunImmoCrawl")


  import Context.spark.implicits._

  val listUrls: Dataset[String] = Database.selectBaseUrls.flatMap { urlR: Row =>
    val urlS = urlR.getString(0)
    val l = ListPage.load(new URL(urlS))
    val nbPages = (l.getTotalAnnouncesCount / l.getAnnouncesUrl.size)
    //(1 to nbPages).map(urlS + "p-" + _)
    (1 to nbPages).map(urlS + "&page=" + _)
  }

  listUrls.take(5).foreach(println)

  val detailsUrls: Dataset[String] = listUrls.flatMap { url: String => ListPage.load(new URL(url)).getAnnouncesUrl }

  detailsUrls.take(5).foreach(println)

  detailsUrls.foreach { url: String =>
    Database.insertLastSeen(url)
    val a = AnnouncePage.load(new URL(url))
    Database.insertAnnounceDetail(url,a)
  }

  Database.selectAnnounceDetail.show()

  Database.selectAnnounceDetail.coalesce(1).write.option("header",true).csv("spark/announces.csv")

  Context.spark.close
}
