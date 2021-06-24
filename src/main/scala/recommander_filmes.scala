import main._
import spark.sqlContext
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.sql.DataFrame

object recommander_filmes {
  import spark.implicits._
  def recommend_films(recommander:  ALSModel, users: DataFrame,movies_titres : DataFrame )= {
  val userSubsetRecs = recommander.recommendForUserSubset(users, 5)
  //userSubsetRecs.printSchema()
  //userSubsetRecs.show()
  //userSubsetRecs.select(col("recommendations").getItem(0)).show()
  //userSubsetRecs.select(col("recommendations").getItem(0)).printSchema()
  //userSubsetRecs.select(col("recommendations").getItem(0).getField("movieId")).show()
  val userSubsetRecs2 = userSubsetRecs.withColumn("movieID",
    $"recommendations".getItem(0).getField("movieId"))
    .withColumn("rating", $"recommendations".getItem(0).getField("rating"))
  // jointure pour afficher les films recommander
  userSubsetRecs2.createOrReplaceTempView("userSubsetRecs2V")
  movies_titres.createOrReplaceTempView("movies_titresV")
  val movies_rec = sqlContext.sql(
    "select  m.userId, m.movieID, m.rating, mt.title " +
      "from userSubsetRecs2V m inner join  movies_titresV mt " +
      "where m.movieID = mt.movieId")
    users.createOrReplaceTempView("users")
    ratingsDS.createOrReplaceTempView("ratingsDS")
    val movies_TOP_ID = sqlContext.sql(
      "select m.userId, m.rating, m.movieID " +
        "from ratingsDS m inner join  users mt " +
        "where m.userId = mt.userId AND m.rating >= 4 ")
    movies_TOP_ID.createOrReplaceTempView("movies_TOP_ID")
    val movies_TOP_R = sqlContext.sql(
      "select m.userId, m.rating, m.movieID, mt.title " +
        "from movies_TOP_ID m inner join  movies_titresV mt " +
        "where m.movieID = mt.movieId ")
    println("Top Rated Movies for chosen users")
    movies_TOP_R.show()
    movies_rec
}
}
