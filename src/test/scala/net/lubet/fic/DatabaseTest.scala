package net.lubet.fic

import java.net.URL

import net.lubet.fic.lbc.AnnouncePage
import org.scalatest.FunSuite

class DatabaseTest extends FunSuite {

  test("testLoad") {
    assert(Database.selectBaseUrls.count() > 0)
  }

  test("Insert cache") {
    val df1Nb = Database.selectAnnounceCache.count()
    val url="https://www.leboncoin.fr/ventes_immobilieres/1476063628.htm/"
    val a = AnnouncePage.load(new URL(url))
    Database.insertAnnounceCache(url,a.toHtml)
    val df2=Database.selectAnnounceCache
    df2.show()

    assertResult(df1Nb+1 )(df2.count())

  }

  test ("insert detail") {
    val df1Nb = Database.selectAnnounceDetail.count()
    val url="https://www.leboncoin.fr/ventes_immobilieres/1476063628.htm/"
    val a = AnnouncePage.load(new URL(url))
    Database.insertAnnounceDetail(url,a)
    val df2=Database.selectAnnounceDetail
    df2.show()

    assertResult(df1Nb+1 )(df2.count())
  }
}
