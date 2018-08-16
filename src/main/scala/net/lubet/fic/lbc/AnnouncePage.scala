package net.lubet.fic.lbc

import java.io.File
import java.net.URL

import net.ruippeixotog.scalascraper.browser.{Browser, JsoupBrowser}
import net.ruippeixotog.scalascraper.dsl.DSL._
import net.ruippeixotog.scalascraper.scraper.ContentExtractors._

import scala.io.Source

object AnnouncePage {
  def load(url: URL): AnnouncePage = {
    new AnnouncePage(JsoupBrowser().get(url.toString))
  }

  def load(source: Source): AnnouncePage = {
    new AnnouncePage(JsoupBrowser().parseString(source.getLines.mkString))
  }

  def load(file: File): AnnouncePage = {
    new AnnouncePage(JsoupBrowser().parseFile(file.getPath))
  }
}

class AnnouncePage(val doc: Browser#DocumentType) {
  lazy val toHtml = doc.toHtml

  lazy val toJSON = {

  }

  def getTitle: Option[String] = {
    doc >?> text("h1._1KQme")
  }

  def getPrice: Option[Long] = {
    doc >?> text("div[data-qa-id=adview_price] span._1F5u3") match {
      case Some(l) => Option(l.replace(" ", "").replace("€", "").toLong)
      case None => None
    }
  }


  def getPublishDate: Option[String] = {
    doc >?> text("div[data-qa-id=adview_date]")
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

  def getCity: Option[String] = {
    doc >?> text("div[data-qa-id=adview_location_informations] span")
  }

  def getChargesIncluded:Option[String] ={
    doc >?> text("div[data-qa-id=criteria_item_charges_included] div._3Jxf3")
  }

  def getFurnished:Option[String]={
    doc >?> text("div[data-qa-id=criteria_item_furnished] div._3Jxf3")
  }

  def getEnergyRate:Option[String]={
    doc >?> text("div[data-qa-id=criteria_item_energy_rate] div._1sd0z")
  }

  def getGES:Option[String]={
    doc >?> text("div[data-qa-id=criteria_item_ges] div._1sd0z")
  }

  def getDepartment: Option[String] = {
    //doc >> text("div[data-qa-id=breadcrumb-item-1] a")
    Option("")
  }

}
