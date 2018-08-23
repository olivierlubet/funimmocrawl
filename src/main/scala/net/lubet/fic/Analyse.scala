package net.lubet.fic

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.ml.regression.{GBTRegressionModel, GBTRegressor}
import org.apache.spark.sql.DataFrame

object Analyse {

  import Context.spark._

  sql("select list_id,attributes as attributes from announce_unique").
    selectExpr("list_id as list_id", "explode(attributes) as attributes").
    selectExpr("list_id", "attributes.*").
    createOrReplaceTempView("announce_attribute")

  sql("""
        |select a.list_id,price[0] as price, category_name, location.lat, location.lng,
        | int(at_square.value) as square,
        | at_ges.value as ges,
        | int(at_rooms.value) as rooms,
        | at_furnished.value_label as furnished,
        | at_rst.value_label as real_estate_type,
        | at_ci.value_label as charges_included,
        | at_fai.value_label as fai_included,
        | at_energy.value as energy_rate
        | from announce_unique as a
        | left join announce_attribute as at_square on a.list_id=at_square.list_id and at_square.key='square'
        | left join announce_attribute as at_ges on a.list_id=at_ges.list_id and at_ges.key='ges'
        | left join announce_attribute as at_rooms on a.list_id=at_rooms.list_id and at_rooms.key='rooms'
        | left join announce_attribute as at_furnished on a.list_id=at_furnished.list_id and at_furnished.key='furnished'
        | left join announce_attribute as at_rst on a.list_id=at_rst.list_id and at_rst.key='real_estate_type'
        | left join announce_attribute as at_ci on a.list_id=at_ci.list_id and at_ci.key='charges_included'
        | left join announce_attribute as at_fai on a.list_id=at_fai.list_id and at_fai.key='fai_included'
        | left join announce_attribute as at_energy on a.list_id=at_energy.list_id and at_energy.key='energy_rate'
      """.stripMargin).createOrReplaceTempView("announce_dataset")

  val data_raw = sql("""
                       |select a.list_id,price[0] as price, category_name, location.lat, location.lng,
                       | int(at_square.value) as square,
                       | at_rst.value_label as real_estate_type
                       | from announce_unique as a
                       | left join announce_attribute as at_square on a.list_id=at_square.list_id and at_square.key='square'
                       | left join announce_attribute as at_rst on a.list_id=at_rst.list_id and at_rst.key='real_estate_type'
                     """.stripMargin).
    na.fill(Map(
    "category_name"->"unknown",
    "real_estate_type"->"unknown"
  )).na.drop


  val indexed_1 = new StringIndexer().
    setInputCol("category_name").
    setOutputCol("categoryIndex").
    setHandleInvalid("skip").
    fit(data_raw).
    transform(data_raw)

  val indexed_2= new StringIndexer().
    setInputCol("real_estate_type").
    setOutputCol("real_estate_typeIndex").
    setHandleInvalid("skip").
    fit(indexed_1).
    transform(indexed_1)



  //indexed_2.show(5)

  val data= new VectorAssembler().
    setInputCols(Array("categoryIndex","lat","lng","square","real_estate_typeIndex")).
    setOutputCol("features").
    transform(indexed_2)


  data.select("features", "price").show(5)

  val featureIndexer = new VectorIndexer().
    setInputCol("features").
    setOutputCol("indexedFeatures").
    setMaxCategories(10).
    fit(data)

  // Split the data into training and test sets (30% held out for testing).
  val Array(trainingData, testData) = data.randomSplit(Array(0.07, 0.03))

  // Train a GBT model.
  val gbt = new GBTRegressor().
    setLabelCol("price").
    setFeaturesCol("indexedFeatures").
    setMaxIter(10)

  // Chain indexer and GBT in a Pipeline.
  val pipeline = new Pipeline().
    setStages(Array(featureIndexer, gbt))

  // Train model. This also runs the indexer.
  val model = pipeline.fit(trainingData)

  // Make predictions.
  val predictions: DataFrame = model.transform(testData)

  // Select example rows to display.
  predictions.select("prediction", "price", "features").show(5)

  // Select (prediction, true label) and compute test error.
  val evaluator = new RegressionEvaluator().
    setLabelCol("price").
    setPredictionCol("prediction").
    setMetricName("rmse")
  val rmse = evaluator.evaluate(predictions)
  println(s"Root Mean Squared Error (RMSE) on test data = $rmse")

  val gbtModel = model.stages(1).asInstanceOf[GBTRegressionModel]
  println(s"Learned regression GBT model:\n ${gbtModel.toDebugString}")
}
