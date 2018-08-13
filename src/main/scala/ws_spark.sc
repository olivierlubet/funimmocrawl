import net.lubet.fic.lbc.ListPage
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}

//val warehouseLocation = "file:"+System.getProperty("user.dir")+"/spark"
val warehouseLocation = "file:///Users/olivi/IdeaProjects/funimmocrawl/spark"
Logger.getLogger("org").setLevel(Level.OFF)
Logger.getLogger("akka").setLevel(Level.OFF)

val spark = SparkSession.builder.master("local").appName("Test").config("spark.sql.warehouse.dir", warehouseLocation).getOrCreate()


spark.catalog.listDatabases.show(false)
spark.catalog.listTables.show(false)

val buDF = spark.read.option("header", true).csv("spark/baseurl.csv")
buDF.first().getString(0)

import spark.implicits._

val buPagesDF = buDF.map { case u: Row => (u.getString(0), ListPage.load(u.getString(0)).toHtml) }

buPagesDF.write.parquet("spark/baseurlHtml")

spark.close()