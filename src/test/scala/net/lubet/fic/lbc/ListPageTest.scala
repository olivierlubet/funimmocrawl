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
    val l = ListPage.load(Source.fromInputStream(getClass.getResourceAsStream("/lbc.listpage.html")))
    assertResult("Ventes immobilières, maisons à vendre Ile-de-France - nos annonces leboncoin")(l.doc >> text("title"))
  }
}
