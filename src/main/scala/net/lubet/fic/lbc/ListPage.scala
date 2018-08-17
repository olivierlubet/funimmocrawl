package net.lubet.fic.lbc

import java.io.File
import java.net.URL

import net.ruippeixotog.scalascraper.browser.{Browser, JsoupBrowser}
import net.ruippeixotog.scalascraper.scraper.ContentExtractors.{attr, text}
import net.ruippeixotog.scalascraper.dsl.DSL.Extract._
import net.ruippeixotog.scalascraper.dsl.DSL._
import net.ruippeixotog.scalascraper.model.Element

import scala.io.Source

object ListPage {
  def load(url: URL): ListPage= {
      new ListPage(JsoupBrowser().get(url.toString))
  }

  def load(source:Source):ListPage={
    new ListPage(JsoupBrowser().parseString(source.getLines.mkString))
  }

  def load(file:File):ListPage={
    new ListPage(JsoupBrowser().parseFile(file.getPath))
  }
}

class ListPage (val doc : Browser#DocumentType ){
 val toHtml: String = doc.toHtml

  lazy val getTotalAnnouncesCount : Int = {
    (doc >> text("span._2ilNG")).replace(" ","").toInt
  }

  lazy val getAnnouncesUrl : List[String] = {
    (doc >> elementList("a.clearfix.trackable"))
      .map { e: Element =>
        "https://www.leboncoin.fr" + (e >> attr("href"))
      }
  }
}