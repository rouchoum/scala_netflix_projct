import Reader_Utils._
import main.sc
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.Dataset

object traitement_cross_val {


  def train_validate_model(ratingDS : Dataset[Rating]): ALSModel = {
    //splitter les données en train et test

    val Array(train, test) = ratingDS.randomSplit(Array(0.8,0.2), seed = 100)
    train.repartition(8).cache()
    //recherche du meilleur modèle avec grid search et cross validation
    val evaluator = new RegressionEvaluator().setMetricName("rmse").setLabelCol("rating").setPredictionCol("prediction")
    val als = new ALS()
      .setColdStartStrategy("drop").
      setUserCol("userId").setItemCol("movieId").setRatingCol("rating").setCheckpointInterval(2)


    //model hyperparameters grid search build
    val ranks = Array(15 , 30)
    val regParams = Array(0.05,0.1)
    val maxIterations = Array(20,30)
    val paramGrid = new ParamGridBuilder()
      .addGrid(als.regParam, regParams)
      .addGrid(als.rank, ranks)
      .addGrid(als.maxIter,maxIterations)
      .build()
    //cross validatiob
    println("cross_validation")
    sc.setCheckpointDir("checkpoint/")
    val cv = new CrossValidator()
      .setEstimator(als)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(5)
    println("cv finished")
    //train model
    val cvModel = cv.fit(train)
    println("model_found")
    val train_bestRmse = cv.getEvaluator.evaluate(cvModel.transform(train))
    //prediction
    val cvPrediction = cvModel.transform(test)
    val cvRmse = cv.getEvaluator.evaluate(cvPrediction)
    printf("The RMSE of the bestModel from CrossValidator on validation set is %3.4f\n", train_bestRmse)
    printf("The RMSE of the bestModel from CrossValidator on test set is %3.4f\n", cvRmse)
    //affichage des meilleurs paramètres
    val descArr = (cvModel.getEstimatorParamMaps zip cvModel.avgMetrics).sortBy(_._2)
    val bestParamMap = descArr(0)._1
    println(s"The best ALS model obtained from CVModel was trained with param = ${bestParamMap}")
    //entrainer meilleur modèle sur toutes les données
    val augModelFromCv = als.fit(ratingDS, bestParamMap)
    return augModelFromCv
   }
  
  }




