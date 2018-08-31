package net.lubet.fic

import java.net.URL

import net.lubet.fic.lbc.JsonListPage
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object App extends App {
  println("FunImmoCrawl")
  val spark = Context.spark
  val sc = spark.sparkContext

  val format = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
  val partition = format.format(new java.util.Date())

  import Context.spark.implicits._

  println("Listing pages to crawl for announces")
  val listUrls: Dataset[String] =
    Database.selectBaseUrls.flatMap { urlR: Row =>
      val urlS = urlR.getString(0)
      val l = JsonListPage.load(new URL(urlS))
      val nbPages = l.totalAnnouncesCount / l.announces.size
      println(s"$nbPages to crawl !!!")
      //(1 to nbPages).map(urlS + "p-" + _)
      (1 to nbPages).map(urlS + "&page=" + _)
    }
  println("Repartitionning")
  listUrls.repartition(4)
  println("Listing announces URL")

  listUrls.foreach { url: String =>
    println(s"Downloading $url")
    try {
      spark.read.json(
        spark.createDataset(
          JsonListPage.load(new URL(url)).announces
        )
      )
        .withColumn("part", lit(partition))
        .repartition($"part")
        .write
        .mode(SaveMode.Append)
        .json("spark/announce_json")
    }
    catch {
      case e: Throwable =>
        println(s"Error with $url", e)
        List.empty
    }
  }

  println("Preparing data")
  val df = Context.spark.read.json("spark/announce_json")
  df.createOrReplaceTempView("announce")

  val df_p = df
  val df_tojoin = df_p.
    groupBy($"list_id").
    agg(
      min($"part").alias("first_seen"),
      max($"part").alias("last_seen")
    )

  //df.printSchema
  // Objectif : récupérer la plus ancienne et la plus récente date où l'on a récupéré l'annonce
  val df_u = df
    .drop("part")
    .distinct
    .join(df_tojoin.select(), df.col("list_id") === df_tojoin.col("list_id"))


  df_u.createOrReplaceTempView("announce_unique")
  df_u.coalesce(1).write.mode(SaveMode.Overwrite).json("spark/announce_json_unique")
  //val df_u = Context.spark.read.json("spark/announce_json_unique")

  println("Closing spark")
  Context.spark.close

  println("Job done !!!")

}

/*
  OLD APP

  println("Loading URL already parsed")
  val announces: DataFrame = Database.selectAnnounceSeen.cache()


  println("Listing pages to crawl for announces")
  val listUrls: Dataset[String] =
    Database.selectBaseUrls.flatMap { urlR: Row =>
      val urlS = urlR.getString(0)
      val l = ListPage.load(new URL(urlS))
      val nbPages = l.totalAnnouncesCount / l.announces.size
      //(1 to nbPages).map(urlS + "p-" + _)
      (1 to nbPages).map(urlS + "&page=" + _)
    }

  println("Listing announces URL")
  val detailsUrls: Dataset[String] = listUrls
    .flatMap {
      url: String =>
        try {
          ListPage.load(new URL(url)).getAnnouncesUrl
        } catch {
          case e: Throwable =>
            println(s"Error with $url", e)
            List.empty
        }
    }

  println("Working on announces")
  detailsUrls.foreach { url: String =>
    println(url)
    // Dans tous les cas, on garde trace d'avoir vu l'annonce
    Database.insertLastSeen(url)
    // Si la page n'a pas encore été parsée, on fait le travail
    if (announces.filter($"url" === url).count() == 0) {
      try {
        val a = AnnouncePage.load(new URL(url))
        Database.insertAnnounceDetail(url, a)
      } catch {
        case e: Throwable =>
          println(s"Error with $url", e)
      }
    }


  println("Recording announces in a single file")
  Database
    .selectAnnounceDetail
    .coalesce(1)
    .write
    .mode(SaveMode.Overwrite)
    .option("header", true)
    .csv("spark/announces.csv")

  */