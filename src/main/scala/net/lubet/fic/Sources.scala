package net.lubet.fic

import org.apache.spark.sql.{DataFrame, SparkSession}

object BaseurlDF {
  def load(spark: SparkSession): DataFrame = {
    spark.read.option("header", true).csv("spark/baseurl.csv")
  }
}
