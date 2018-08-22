package net.lubet.fic.lbc

import java.net.URL

import org.scalatest.FunSuite

import scala.io.Source

class JsonListPageTest extends FunSuite {
  test("load URL") {
    val l = JsonListPage.load(new URL("https://www.leboncoin.fr/recherche/?category=8&regions=12"))
    assertResult("Immobilier Ile-de-France - nos annonces leboncoin")(l.doc.title())
  }

  test("load File") {
    val l = JsonListPage.load(Source.fromInputStream(getClass.getResourceAsStream("/lbc.listpage.html"), "UTF-8"))
    assertResult("Immobilier Ile-de-France - nos annonces leboncoin")(l.doc.title())
  }

  test("total pages") {
    val l = JsonListPage.load(Source.fromInputStream(getClass.getResourceAsStream("/lbc.listpage.html"), "UTF-8"))
    assertResult(124217)(l.totalAnnouncesCount)
  }


}
