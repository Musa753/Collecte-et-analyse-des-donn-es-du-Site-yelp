import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import java.util.Properties

class ETL_TIME(oracleConnectionProperties: Properties, oracleJdbcUrl: String, userDF: DataFrame) {

  def extract(): DataFrame = {
     userDF.select("user_id", "yelping_since")
  }

  def transform(): DataFrame = {
     val transformedDF = userDF.select("yelping_since","user_id")
      .withColumn("yelping_year", year(col("yelping_since")))
      .withColumn("yelping_month", month(col("yelping_since")))
      .withColumn("yelping_day", dayofmonth(col("yelping_since")))

     val uniqueKeyDF = transformedDF.withColumn("date_id", monotonically_increasing_id)

    uniqueKeyDF

  }

  def load(): Unit = {
     transform().drop("user_id").dropDuplicates(Seq("yelping_year","yelping_month","yelping_day")).write
      .mode(SaveMode.Overwrite)
      .jdbc(oracleJdbcUrl, "TIME", oracleConnectionProperties)
  }

  def joined(): DataFrame = {
     transform().select("user_id", "date_id")
  }
}
