package net.lubet.fic.lbc

import java.net.URL

import net.ruippeixotog.scalascraper.dsl.DSL.Extract._
import net.ruippeixotog.scalascraper.dsl.DSL._
import org.scalatest.FunSuite

import scala.io.Source

class AnnouncePageTest extends FunSuite {
  test("load URL") {
    val a = AnnouncePage.load(new URL("https://www.leboncoin.fr/ventes_immobilieres/1476063628.htm/"))
    assertResult("Belle maison lumineuse dans quartier calme")(a.doc >> text("title"))
  }

  test("load File") {
    val a = AnnouncePage.load(Source.fromInputStream(getClass.getResourceAsStream("/lbc.announcepage.vente.html"), "UTF-8"))
    assertResult("Belle maison lumineuse dans quartier calme")(a.doc >> text("title"))
  }


  test("elements vente") {
    val a = AnnouncePage.load(Source.fromInputStream(getClass.getResourceAsStream("/lbc.announcepage.vente.html"), "UTF-8"))
    assertResult(269000)(a.getPrice.get)
    assertResult("Belle maison lumineuse dans quartier calme")(a.getTitle.get)
    assertResult("14/08/2018 à 23h59")(a.getPublishDate.get)
    assertResult("Maison")(a.getType.get)
    assertResult(8)(a.getRoomsCount.get)
    assertResult(205)(a.getSurface.get)
    //assertResult(20)(a.getDescription.length)
    assertResult("Sainte-Maure-de-Touraine 37800")(a.getCity.get)
    assertResult("")(a.getDepartment.get)
    assertResult("C")(a.getEnergyRate.get)
    assertResult("A")(a.getGES.get)
  }


  test("elements location") {
    val a = AnnouncePage.load(Source.fromInputStream(getClass.getResourceAsStream("/lbc.announcepage.location.html"), "UTF-8"))
    assertResult(500)(a.getPrice.get)
    assertResult("Appartement lumineux - 2 chambres")(a.getTitle.get)
    assertResult("15/08/2018 à 23h53")(a.getPublishDate.get)
    assertResult("Appartement")(a.getType.get)
    assertResult("Oui")(a.getChargesIncluded.get)
    assertResult("Non meublé")(a.getFurnished.get)
  }
}
