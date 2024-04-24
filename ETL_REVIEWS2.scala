import org.apache.spark.sql.functions.{col, sum}
import org.apache.spark.sql.{DataFrame, SaveMode,SparkSession}

import java.util.Properties

class ETL_REVIEWS2(  oracleConnectionProperties: Properties, oracleJdbcUrl: String,reviewDF: DataFrame) {

  def extract(): DataFrame = {
    // Renvoyer les données complètes du DataFrame initial
    reviewDF
  }

  def joined(): DataFrame = {
    val aggregatedReviewDF = extract().groupBy("user_id")
      .agg(
        sum("useful").alias("Reviews_useful_count"),
        sum("cool").alias("Reviews_cool_count"),
        sum("funny").alias("Reviews_funny_count"),
        sum("stars").alias("Reviews_stars_count"),
      )
     val finalAggregatedReviewDF = extract().join(aggregatedReviewDF,Seq("user_id")).select(
      col("review_id"),
      col("Reviews_useful_count"),
      col("Reviews_cool_count"),
      col("Reviews_funny_count"),
      col("Reviews_stars_count"),
    )
    finalAggregatedReviewDF.show()

    finalAggregatedReviewDF

  }

  def joinedDWBUS(): DataFrame = {
    // Sélectionner uniquement les colonnes "review_id" et "user_id"
    reviewDF.select("review_id", "user_id")
  }

  def load(transformedDF: DataFrame): Unit = {
    // Sauvegarde des données transformées dans la base de données Oracle
    extract()
      .join(transformedDF,Seq("review_id"),"inner")
      .select("review_id","text","date")
      .write
      .mode(SaveMode.Overwrite)
      .jdbc(oracleJdbcUrl, "review", oracleConnectionProperties)
  }
}
