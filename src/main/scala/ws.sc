import net.ruippeixotog.scalascraper.browser.JsoupBrowser
import net.ruippeixotog.scalascraper.dsl.DSL._
import net.ruippeixotog.scalascraper.dsl.DSL.Extract._
import net.ruippeixotog.scalascraper.dsl.DSL.Parse._

val browser = JsoupBrowser()
val doc: browser.DocumentType = browser.get("https://www.leboncoin.fr/ventes_immobilieres/offres/ile_de_france/")

doc >> text("title")

(doc >> element("a.clearfix")).attr("href")
