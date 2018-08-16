package net.lubet.fic

import java.net.URL

import net.lubet.fic.lbc.ListPage
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.FunSuite

class AppTest extends FunSuite {
  val spark = Context.spark

  test("pipeline") {
    import spark.implicits._
    val buPages = Database.selectBaseUrls.map { u: Row => ListPage.load(new URL(u.getString(0))).toHtml }
    assertResult(Database.selectBaseUrls.count())(buPages.count())
  }

  test("init tables") {

    spark.sql("DROP TABLE IF EXISTS test_last_seen")

    spark.sql(
      """
        |CREATE TABLE test_last_seen(
        |url string,
        |date_last_seen date
        |)
        |USING PARQUET
      """.stripMargin
    )
    spark.sql(
      """
        |INSERT INTO test_last_seen VALUES (
        |"test",cast("2018-01-01" as date)
        |)
      """.stripMargin)
    spark.sql("insert into test_last_seen values ('test',now())")

    spark.sql("select * from test_last_seen").show()

  }
}
