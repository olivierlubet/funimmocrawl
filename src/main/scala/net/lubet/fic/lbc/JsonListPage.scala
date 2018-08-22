package net.lubet.fic.lbc

import java.io.File
import java.net.URL

import org.json4s._
import org.json4s.native.JsonMethods._
import org.jsoup.Jsoup
import org.jsoup.nodes.Document

import scala.io.Source
import scala.util.matching.Regex

object JsonListPage {
  def load(url: URL): JsonListPage = {
    new JsonListPage(Browser.get(url))
  }

  def load(source: Source): JsonListPage = {
    new JsonListPage(Jsoup.parse(source.getLines.mkString))
  }

  def load(file: File): JsonListPage = {
    new JsonListPage(Jsoup.parse(file, "UTF-8"))
  }
}

class JsonListPage(val doc: Document) {
  val html = doc.html()

  lazy val totalAnnouncesCount: Int = {
    findFirstGroupIn(""""total":([^,]+)."""".r) match {
      case Some(s) => s.toInt
      case None => 0
    }
  }

  lazy val data: String = {
    findFirstGroupIn("""<script>window.FLUX_STATE =(.+)</script>""".r) match {
      case Some(s) => s
      case None => ""
    }
  }

  lazy val announces: List[String] = {
    (parse(data) \ "adSearch" \ "data" \ "ads" ).children.map {
      j => compact(render(j))
    }
  }

  def findFirstGroupIn(r: Regex): Option[String] = {
    r.findFirstMatchIn(html) match {
      case Some(e) => Option(e.group(1))
      case None => None
    }
  }
}
