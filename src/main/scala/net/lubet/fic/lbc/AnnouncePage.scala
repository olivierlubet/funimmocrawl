package net.lubet.fic.lbc

import java.io.File
import java.net.URL

import net.ruippeixotog.scalascraper.browser.{Browser, HtmlUnitBrowser, JsoupBrowser}
import net.ruippeixotog.scalascraper.dsl.DSL._
import net.ruippeixotog.scalascraper.scraper.ContentExtractors._

import scala.io.Source
import scala.util.matching.Regex

object AnnouncePage {
  def load(url: URL): AnnouncePage = {
    new AnnouncePage(HtmlUnitBrowser().get(url.toString))
  }

  def load(source: Source): AnnouncePage = {
    new AnnouncePage(HtmlUnitBrowser().parseString(source.getLines.mkString))
  }

  def load(file: File): AnnouncePage = {
    new AnnouncePage(HtmlUnitBrowser().parseFile(file.getPath))
  }
}

class AnnouncePage(val doc: Browser#DocumentType) {
  lazy val toHtml = doc.toHtml

  def getTitle: Option[String] = {
    doc >?> text("h1._1KQme")
  }

  def getUrl: Option[String] = {
    doc >?> attr("content")("meta[property=og:url]")
  }

  def getPrice: Option[Long] = {
    doc >?> text("div[data-qa-id=adview_price] span._1F5u3") match {
      case Some(l) => Option(l.replace(" ", "").replace("€", "").toLong)
      case None => None
    }
  }


  /**
    * input : String containing 15/08/2018 à 23h53
    * @return String containing a date with ISO8601 format yyyy-mm-dd
    */
  def getPublishDate: Option[String] = {
    val date = raw".*(\d{2})/(\d{2})/(\d{4}).*".r
    doc >?> text("div[data-qa-id=adview_date]") match {
      case Some (d) => d match {
        case date(d,m,y) => Option(s"$y-$m-$d")
      }
      case None => None
    }
  }

  def getType: Option[String] = {
    doc >?> text("div[data-qa-id=criteria_item_real_estate_type] div._3Jxf3")
  }

  def getRoomsCount: Option[Int] = {
    doc >?> text("div[data-qa-id=criteria_item_rooms] div._3Jxf3") match {
      case Some(i) => Option(i.replace(" ", "").toInt)
      case None => None
    }
  }

  def getSurface: Option[Double] = {
    doc >?> text("div[data-qa-id=criteria_item_square] div._3Jxf3") match {
      case Some(d) => Option(d
        .replace(" ", "")
        .replace(",", ".")
        .replace("m²", "")
        .toDouble)
      case None => None
    }

  }

  def getDescription: Option[String] = {
    doc >?> element("meta[name=description") >> attr("content")
  }



  def getChargesIncluded: Option[String] = {
    doc >?> text("div[data-qa-id=criteria_item_charges_included] div._3Jxf3")
  }

  def getFurnished: Option[String] = {
    doc >?> text("div[data-qa-id=criteria_item_furnished] div._3Jxf3")
  }

  def getEnergyRate: Option[String] = {
    doc >?> text("div[data-qa-id=criteria_item_energy_rate] div._1sd0z")
  }

  def getGES: Option[String] = {
    doc >?> text("div[data-qa-id=criteria_item_ges] div._1sd0z")
  }

  def getRegion: Option[String] = {
    findFirstGroupIn(""""region_name":"([^"]+)"""".r)
  }

  def getDepartment: Option[String] = {
    findFirstGroupIn(""""department_name":"([^"]+)"""".r)
  }

  def getCity: Option[String] = {
    //doc >?> text("div[data-qa-id=adview_location_informations] span")
    findFirstGroupIn(""""city":"([^"]+)"""".r)
  }

  def getZipCode: Option[String] = {
    findFirstGroupIn(""""zipcode":"([^"]+)"""".r)
  }

  def getLat: Option[String] = {
    findFirstGroupIn(""""lat":([^,]+)."""".r)
  }
  def getLng: Option[String] = {
    findFirstGroupIn(""""lng":([^,]+)."""".r)
  }

  def getCategory: Option[String] = {
    findFirstGroupIn(""""category_name":"([^"]+)"""".r)
  }

  def findFirstGroupIn(r: Regex): Option[String] = {
    r.findFirstMatchIn(toHtml) match {
      case Some(e) => Option(e.group(1))
      case None => None
    }
  }
}
