import net.lubet.fic._
import net.lubet.fic.lbc._

Context.spark
import Context.spark._

val df_u = Context.spark.read.json("spark/announce_json_unique")
df_u.show()
