package net.lubet.fic

import java.net.URL

import net.lubet.fic.lbc.AnnouncePage
import org.scalatest.FunSuite

import scala.io.Source

class DatabaseTest extends FunSuite {

  test("testLoad") {
    assert(Database.selectBaseUrls.count() > 0)
  }


  test ("insert detail") {
    val df1Nb = Database.selectAnnounceDetail.count()
    val a = AnnouncePage.load(Source.fromInputStream(getClass.getResourceAsStream("/lbc.announcepage.vente.html"), "UTF-8"))
    Database.insertAnnounceDetail("test",a)
    val df2=Database.selectAnnounceDetail
    df2.show()

    assertResult(df1Nb+1 )(df2.count())
  }
}
