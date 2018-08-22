package net.lubet.fic.lbc

import java.net.URL

import net.ruippeixotog.scalascraper.dsl.DSL.Extract._
import net.ruippeixotog.scalascraper.dsl.DSL._
import org.scalatest.FunSuite

import scala.io.Source

class ListPageTest extends FunSuite {

  test("load URL") {
    val l = ListPage.load(new URL("https://www.leboncoin.fr/ventes_immobilieres/offres/ile_de_france/"))
    assertResult("Ventes immobilières, maisons à vendre Ile-de-France - nos annonces leboncoin")(l.doc >> text("title"))
  }

  test("load File") {
    val l = ListPage.load(Source.fromInputStream(getClass.getResourceAsStream("/lbc.listpage.html"), "UTF-8"))
    assertResult("Ventes immobilières, maisons à vendre Ile-de-France - nos annonces leboncoin")(l.doc >> text("title"))
  }

  test("page number") {
    val l = ListPage.load(Source.fromInputStream(getClass.getResourceAsStream("/lbc.listpage.html"), "UTF-8"))

    assertResult("124126")(l.getTotalAnnouncesCount)
  }

  test("announces") {
    val l = ListPage.load(Source.fromInputStream(getClass.getResourceAsStream("/lbc.listpage.html"), "UTF-8"))
    val items = l.doc >> elementList("a.clearfix.trackable")
    assertResult(35)(items.size)
    assertResult("/ventes_immobilieres/1105277901.htm/")(items.head >> attr("href"))

    assertResult(35)(l.getAnnouncesUrl.size)
    assertResult("https://www.leboncoin.fr/ventes_immobilieres/1105277901.htm/")(l.getAnnouncesUrl.head)
  }
}
