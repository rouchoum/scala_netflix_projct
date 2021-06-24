
import org.apache.spark.sql.types._

object Reader_Utils {
  val user_col = "userId"
  val movie_col="movieId"
  val rating_col = "rating"
  val time_col = "timestamp"
  val year_col = "year"
  val title_col = "title"
  val numPartitions = 8

  case class Rating(userId: Int, movieId: Int, rating: Double)
  val schema = new StructType()
    .add(user_col, IntegerType, true)
    .add(movie_col, IntegerType, true)
    .add(rating_col, DoubleType, true)



  val schema_csv = new StructType()
    .add(user_col, IntegerType, true)
    .add(rating_col, DoubleType, true)
    .add(time_col, DateType, true)

  val schema_titre = new StructType()
    .add(movie_col, IntegerType)
    .add(year_col, IntegerType)
    .add(title_col, StringType)
}



