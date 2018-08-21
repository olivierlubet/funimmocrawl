package net.lubet.fic.lbc

import java.io.File
import java.net.URL

import net.ruippeixotog.scalascraper.browser.{Browser, HtmlUnitBrowser, JsoupBrowser}
import net.ruippeixotog.scalascraper.scraper.ContentExtractors.{attr, text}
import net.ruippeixotog.scalascraper.dsl.DSL.Extract._
import net.ruippeixotog.scalascraper.dsl.DSL._
import net.ruippeixotog.scalascraper.model.Element
import org.jsoup.Jsoup
import org.jsoup.nodes.Document

import scala.io.Source
import scala.util.matching.Regex

object ListPage {
  def load(url: URL): ListPage = {
    new ListPage(Browser.get(url))
  }

  def load(source: Source): ListPage = {
    Jsoup.parse(source.getLines.mkString)
  }

  def load(file: File): ListPage = {
    Jsoup.parse(file, "UTF-8")
  }
}

class ListPage(val doc: Document) {
  val toHtml: String = doc.html()

  lazy val getTotalAnnouncesCount: Int = {
    findFirstGroupIn(""""total":([^,]+)."""".r) match {
      case Some(s) => s.toInt()
      case None => 0
    }
  }

  lazy val getAnnouncesUrl : List[String] = {
    (doc >> elementList("a.clearfix.trackable"))
      .map { e: Element =>
        "https://www.leboncoin.fr" + (e >> attr("href"))
      }
  }

  def findFirstGroupIn(r: Regex): Option[String] = {
    r.findFirstMatchIn(toHtml) match {
      case Some(e) => Option(e.group(1))
      case None => None
    }
  }
}