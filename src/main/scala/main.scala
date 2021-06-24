import Reader_Utils._
import org.apache.spark.sql.{Dataset, SparkSession}
import recommander_filmes.recommend_films
import traitement_cross_val._

object main extends App {
  // spark session creation
  val spark: SparkSession = SparkSession.builder()
    .master("local[8]")
    .appName("Sparkex")
    .getOrCreate()
  spark.conf.set("spark.sql.shuffle.partitions", 60)
  val sc = spark.sparkContext
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._

  // Reading csv files
  val df = spark.read.options(Map("delimiter" -> ",")).schema(schema)
    .csv("netflix_1.csv", "netflix_2.csv")
  // reading movies titles file :
  val movies_titres = spark.read.options(Map("delimiter" -> ",")).schema(schema_titre)
    .csv("movie_titles.csv")
  // ratings and movies_title table creation
  val ratingsDS: Dataset[Rating] = df.as[Rating]
  ratingsDS.cache()
  // Generate top 10 movie recommendations for a specified set of users
  val users = ratingsDS.select("userId").distinct().limit(5)

  //getting best model
  println("****cross validation***")
  val recommander = train_validate_model(ratingsDS)

  // recommend movies
  val userSubsetRecs = recommander.recommendForUserSubset(users, 5)
  val recmovies = recommend_films(recommander, users, movies_titres)

  //val users_all = ratingsDS.filter(ratingsDS("userId")===users("userId")).orderBy(desc("rating"))
 // users_all.createOrReplaceTempView("users_all")

  println("recommended movies")
  recmovies.show(100)
  //println("lasst")
  //movies_TOP_R.filter(movies_TOP_R("userId")===recmovies("userId")).show(100)









}
