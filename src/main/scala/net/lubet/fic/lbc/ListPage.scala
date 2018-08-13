package net.lubet.fic.lbc

import java.io.File
import java.net.URL

import net.ruippeixotog.scalascraper.browser.{Browser, JsoupBrowser}

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
 val toHtml = doc.toHtml
}