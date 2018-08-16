package net.lubet.fic

import net.lubet.fic.lbc.AnnouncePage
import org.apache.spark.sql.DataFrame


object Database {

  Context.spark.sql(
    """
      |CREATE TABLE IF NOT EXISTS last_seen(
      |url string,
      |date_last_seen date
      |)
      |USING PARQUET
    """.stripMargin)


  Context.spark.sql(
    """
      |CREATE TABLE IF NOT EXISTS announce_cache(
      |url string,
      |date_cache date,
      |html string
      |)
      |USING PARQUET
    """.stripMargin)


  Context.spark.sql("DROP table if exists announce")

  Context.spark.sql(
    """
      |CREATE TABLE IF NOT EXISTS announce(
      |url string,
      |date_record date,
      |title string,
      |type string,
      |date_published string,
      |price string,
      |room_count string,
      |surface string,
      |city string,
      |charges_included string,
      |furnished string,
      |energy_rate string,
      |gez string
      |)
      |USING PARQUET
    """.stripMargin)

  def selectBaseUrls: DataFrame = {
    Context.spark.read.option("header", value = true).csv("spark/baseurl.csv")
  }

  def insertLastSeen(url:String): Unit = {
    Context.spark.sql(
      s"""
        |INSERT INTO last_seen VALUES ("$url",now())
      """.stripMargin)
  }

  // Non satisfaisant dans le sens ou il n'existe pas d'opérationinverse à xml.Utility.escape
  def insertAnnounceCache(url:String, html:String): Unit ={
    Context.spark.sql(
      s"""
         |INSERT INTO announce_cache VALUES ("$url",now(),"${xml.Utility.escape(html)}")
      """.stripMargin)
  }

  def insertAnnounceDetail (url:String, a:AnnouncePage): Unit = {
    import Context.spark.implicits._
    Context.spark.sql(
      s"""
         |INSERT INTO announce VALUES ("$url",now(),
         |"${a.getTitle.getOrElse("")}",
         |"${a.getType.getOrElse("")}",
         |"${a.getPublishDate.getOrElse("")}",
         |"${a.getPrice.getOrElse("")}",
         |"${a.getRoomsCount.getOrElse("")}",
         |"${a.getSurface.getOrElse("")}",
         |"${a.getCity.getOrElse("")}",
         |"${a.getChargesIncluded.getOrElse("")}",
         |"${a.getFurnished.getOrElse("")}",
         |"${a.getEnergyRate.getOrElse("")}",
         |"${a.getGES.getOrElse("")}"
         |)
      """.stripMargin)
  }

  def selectAnnounceDetail: DataFrame = {
    Context.spark.sql(
      """
        |SELECT * FROM announce
      """.stripMargin)
  }

  def selectAnnounceCache: DataFrame = {
    Context.spark.sql(
      """
        |SELECT * FROM announce_cache
      """.stripMargin)
  }
}
