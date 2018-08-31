package net.lubet.fic

import org.apache.spark.ml._
import org.apache.spark.ml.evaluation._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.regression._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object Analyse {

  import Context.spark._
  import Context.spark.implicits._

  sql("select list_id,attributes as attributes from announce_unique").
    selectExpr("list_id as list_id", "explode(attributes) as attributes").
    selectExpr("list_id", "attributes.*").
    createOrReplaceTempView("announce_attribute")

  sql(
    """
      |create table
      |if not exists announce_attribute
      |as
      |select * from announce_attribute_view
    """.stripMargin
  )

  sql(
    """
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

  val data_raw = sql(
    """
      |select a.list_id,
      |location.department_name,
      |location.city_label,
      | price[0] as price,
      | int(at_square.value) as square,
      | int(price[0]) / int(at_square.value) as price_per_square,
      | at_ret.value_label as real_estate_type,
      | int(at_rooms.value) as rooms,
      | at_furnished.value_label as furnished,
      | at_ci.value_label as charges_included
      | from announce_unique as a
      | left join announce_attribute as at_square on a.list_id=at_square.list_id and at_square.key='square'
      | left join announce_attribute as at_ret on a.list_id=at_ret.list_id and at_ret.key='real_estate_type'
      | left join announce_attribute as at_rooms on a.list_id=at_rooms.list_id and at_rooms.key='rooms'
      | left join announce_attribute as at_furnished on a.list_id=at_furnished.list_id and at_furnished.key='furnished'
      | left join announce_attribute as at_ci on a.list_id=at_ci.list_id and at_ci.key='charges_included'
      | where category_id='10' and location.department_id='75' and at_ret.value_label in ('Maison','Appartement')
    """.stripMargin).
    na.fill(Map(
    "real_estate_type" -> "unknown",
    "rooms" -> "1"
  )).na.drop

  /*
 at_fai.value_label as fai_included

   left join announce_attribute as at_fai on a.list_id=at_fai.list_id and at_fai.key='fai_included'

Pourquoi cela plante-t-il ????
   */

  data_raw.write.mode(SaveMode.Overwrite).json("spark/analyse/data_raw")
  data_raw.write.format("com.databricks.spark.csv").mode(SaveMode.ErrorIfExists).option("header","true").save("spark/analyse/data_raw_csv")
  //val data_raw= Context.spark.read.json("spark/analyse/data_raw")
  /*
  val data_raw= Context.spark.read.option("header","true").csv("spark/analyse/data_raw_csv").
  withColumn("square", 'square.cast(IntegerType)).
  withColumn("rooms", 'rooms.cast(IntegerType)).
  withColumn("price", 'price.cast(IntegerType))
  */
  // Split the data into training and test sets (30% held out for testing).
  val Array(trainingData, testData) = data_raw.randomSplit(Array(0.7, 0.3))


  def bigPipeline = {

    val pl = new Pipeline().
      setStages(Array(
        new StringIndexer().
          setInputCol("real_estate_type").
          setOutputCol("real_estate_typeIndex").
          setHandleInvalid("skip")
        ,
        new StringIndexer().
          setInputCol("city_label").
          setOutputCol("cityIndex").
          setHandleInvalid("skip")
        ,
        new StringIndexer().
          setInputCol("furnished").
          setOutputCol("furnishedIndex").
          setHandleInvalid("skip")
        ,
        new StringIndexer().
          setInputCol("charges_included").
          setOutputCol("charges_includedIndex").
          setHandleInvalid("skip")
        ,
/*        new StringIndexer().
          setInputCol("fai_included").
          setOutputCol("fai_includedIndex").
          setHandleInvalid("skip")
        ,*/
        new OneHotEncoderEstimator().
          setInputCols(Array("real_estate_typeIndex", "cityIndex", "furnishedIndex", "charges_includedIndex")).//, "fai_includedIndex")).
          setOutputCols(Array("real_estate_typeVect", "cityVect", "furnishedVect", "charges_includedVect"))//, "fai_includedVect"))
        ,
        new VectorAssembler().
          setInputCols(Array("cityVect", "square", "real_estate_typeVect", "rooms", "furnishedVect", "charges_includedVect")).//, "fai_includedVect")).
          setOutputCol("features")
        ,
        new MinMaxScaler()
          .setInputCol("features")
          .setOutputCol("scaledFeatures")
        ,
        new VectorIndexer().
          setInputCol("scaledFeatures").
          setOutputCol("indexedFeatures").
          setMaxCategories(1500).
          setHandleInvalid("skip")
        ,
        new LinearRegression().
          setMaxIter(100).
          setRegParam(0.3).
          setElasticNetParam(0.8).
          setLabelCol("price").
          setFeaturesCol("indexedFeatures")
      ))
    val model = pl.fit(trainingData)
    val predictions = model.transform(testData)

    predictions.select($"prediction", $"price", $"features", $"prediction" - $"price", $"price_per_square").show(5)

    // Select (prediction, true label) and compute test error.
    val lrEvaluator = new RegressionEvaluator().
      setLabelCol("price").
      setPredictionCol("prediction").
      setMetricName("rmse")
    val lrRmse = lrEvaluator.evaluate(predictions)
    println(s"Root Mean Squared Error (RMSE) on test data = $lrRmse")
    // 411 sur Paris

    val model_final = pl.fit(data_raw)
    model_final .write.overwrite().save("spark/analyse/model/linear-regression")
  }

  def tests = {
    ///

    val indexed = new StringIndexer().
      setInputCol("real_estate_type").
      setOutputCol("real_estate_typeIndex").
      setHandleInvalid("skip").
      fit(data_raw).
      transform(data_raw)

    val indexed2 = new StringIndexer().
      setInputCol("city_label").
      setOutputCol("cityIndex").
      setHandleInvalid("skip").
      fit(indexed).
      transform(indexed)

    val encoded = new OneHotEncoderEstimator().
      setInputCols(Array("real_estate_typeIndex", "cityIndex")).
      setOutputCols(Array("real_estate_typeVect", "cityVect")).
      fit(indexed2).
      transform(indexed2)


    val data = new VectorAssembler().
      setInputCols(Array("cityVect", "square", "real_estate_typeVect")).
      setOutputCol("features").
      transform(encoded)


    data.select("features", "price_per_square", "price", "square").show(5)

    val featureIndexer = new VectorIndexer().
      setInputCol("features").
      setOutputCol("indexedFeatures").
      setMaxCategories(1500). // 1241 villes uniques sur l'ile de france
      fit(data)



    //////// LINEAR REGRESSION


    val lr = new LinearRegression().
      setMaxIter(100).
      setRegParam(0.3).
      setElasticNetParam(0.8).
      setLabelCol("price").
      setFeaturesCol("indexedFeatures")

    // Train model. This also runs the indexer.
    //val lrModel: LinearRegressionModel = lr.fit(trainingData)

    val lrPipeline = new Pipeline().
      setStages(Array(featureIndexer, lr))

    val lrModel: PipelineModel = lrPipeline.fit(trainingData)

    // Make predictions.
    val lrPredictions: DataFrame = lrModel.transform(testData)

    // Select example rows to display.
    lrPredictions.select($"prediction", $"price_per_square", $"features", $"prediction" * $"square", $"price").show(5)

    // Select (prediction, true label) and compute test error.
    val lrEvaluator = new RegressionEvaluator().
      setLabelCol("price").
      setPredictionCol("prediction").
      setMetricName("rmse")
    val lrRmse = lrEvaluator.evaluate(lrPredictions)
    println(s"Root Mean Squared Error (RMSE) on test data = $lrRmse")
    // 2828.2448416726543 france entière
    // 579 sur Paris


    ////// GRADIENT BOOSTED TREE

    // Train a GBT model.
    val gbt = new GBTRegressor().
      setLabelCol("price").
      setFeaturesCol("indexedFeatures").
      setMaxIter(10).
      setMaxBins(1000)

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
    // 20673.725115394882
    // 1185 sur Paris

    val gbtModel = model.stages(1).asInstanceOf[GBTRegressionModel]
    println(s"Learned regression GBT model:\n ${gbtModel.toDebugString}")
  }
}