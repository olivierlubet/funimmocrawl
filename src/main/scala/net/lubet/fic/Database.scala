package net.lubet.fic

import net.lubet.fic.lbc.AnnouncePage
import org.apache.spark.sql.DataFrame


object Database {

  initLastSeen
  initAnnounce

  def initLastSeen: DataFrame = {
    //Context.spark.sql("DROP TABLE IF EXISTS last_seen")

    Context.spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS last_seen(
        |url string,
        |date_last_seen date
        |)
        |USING PARQUET
      """.stripMargin)

  }

  def initAnnounce: DataFrame = {
    //Context.spark.sql("DROP TABLE IF EXISTS announce")

    Context.spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS announce(
        |url string,
        |date_record date,
        |category string,
        |title string,
        |type string,
        |date_published string,
        |price string,
        |room_count string,
        |surface string,
        |charges_included string,
        |furnished string,
        |energy_rate string,
        |gez string,
        |region string,
        |department string,
        |city string,
        |zipcode string,
        |lat string,
        |lng string
        |)
        |USING PARQUET
      """.stripMargin)

  }

  def selectBaseUrls: DataFrame = {
    Context.spark.read.option("header", value = true).csv("spark/baseurl.csv")
  }

  def insertLastSeen(url: String): Unit = {
    Context.spark.sql(
      s"""
         |INSERT INTO last_seen VALUES ("$url",now())
      """.stripMargin)
  }


  def insertAnnounceDetail(url: String, a: AnnouncePage): Unit = {
    Context.spark.sql(
      s"""
         |INSERT INTO announce VALUES ("$url",now(),
         |"${a.getCategory.getOrElse("")}",
         |"${a.getTitle.getOrElse("")}",
         |"${a.getType.getOrElse("")}",
         |"${a.getPublishDate.getOrElse("")}",
         |"${a.getPrice.getOrElse("")}",
         |"${a.getRoomsCount.getOrElse("")}",
         |"${a.getSurface.getOrElse("")}",
         |"${a.getChargesIncluded.getOrElse("")}",
         |"${a.getFurnished.getOrElse("")}",
         |"${a.getEnergyRate.getOrElse("")}",
         |"${a.getGES.getOrElse("")}",
         |"${a.getRegion.getOrElse("")}",
         |"${a.getDepartment.getOrElse("")}",
         |"${a.getCity.getOrElse("")}",
         |"${a.getZipCode.getOrElse("")}",
         |"${a.getLat.getOrElse("")}",
         |"${a.getLng.getOrElse("")}"
         |)
      """.stripMargin)
  }

  def insertAnnounceJson(partition: String, a: String): Unit = {
    Context.spark.sql(
      s"""
         |INSERT INTO announce_json VALUES ("$partition","$a")
      """.stripMargin)
  }
  def selectAnnounceDetail: DataFrame = {
    Context.spark.sql(
      """
        |SELECT * FROM announce
      """.stripMargin)
  }

  def selectAnnounceSeen: DataFrame = {
    Context.spark.sql(
      """
        |SELECT distinct url FROM announce
      """.stripMargin)
  }

  def selectAnnounceCache: DataFrame = {
    Context.spark.sql(
      """
        |SELECT * FROM announce_cache
      """.stripMargin)
  }
}
