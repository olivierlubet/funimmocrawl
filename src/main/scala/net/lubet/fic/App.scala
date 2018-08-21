package net.lubet.fic

import java.net.URL

import net.lubet.fic.lbc.{AnnouncePage, ListPage}
import org.apache.log4j.{Level, Logger}
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql._
import org.jsoup.HttpStatusException

object App extends App  {
  println("FunImmoCrawl")
  import Context.spark.implicits._

  println("Loading URL already parsed")
  val announces: DataFrame = Database.selectAnnounceSeen.cache()

  println("Listing pages to crawl for announces")
  val listUrls: Dataset[String] = Database.selectBaseUrls.flatMap { urlR: Row =>
    val urlS = urlR.getString(0)

    try {
      val l = ListPage.load(new URL(urlS))
      val nbPages = l.getTotalAnnouncesCount / l.getAnnouncesUrl.size
      //(1 to nbPages).map(urlS + "p-" + _)
      (1 to nbPages).map(urlS + "&page=" + _)
    } catch {
      case e:Throwable =>
        println(s"Error with $urlS",e)
        List.empty
    }
  }

  println("Listing announces URL")
  val detailsUrls: Dataset[String] = listUrls
    .flatMap {
      url: String => try {
        ListPage.load(new URL(url)).getAnnouncesUrl
      } catch {
        case e: Throwable =>
          println(s"Error with $url",e)
          List.empty
      }
    }

  detailsUrls.foreach { url: String =>
    // Dans tous les cas, on garde trace d'avoir vu l'annonce
    Database.insertLastSeen(url)
    // Si la page n'a pas encore été parsée, on fait le travail
    if (announces.filter($"url" === url).count() == 0) {
      try {
        val a = AnnouncePage.load(new URL(url))
        Database.insertAnnounceDetail(url,a)
      } catch {
        case e: Throwable =>
          println(s"Error with $url",e)
      }
    }
  }

  println("Recording announces in a single file")
  Database
    .selectAnnounceDetail
    .coalesce(1)
    .write
    .mode(SaveMode.Overwrite)
    .option("header",true)
    .csv("spark/announces.csv")

  println("Closing spark")
  Context.spark.close

  println("Job done !!!")
}
