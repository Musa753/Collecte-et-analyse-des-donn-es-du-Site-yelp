import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._
import java.util.Properties
import org.apache.spark.sql.functions._


class ETL_REVIEWS(oracleConnectionProperties: Properties, oracleJdbcUrl: String, businessDF: DataFrame, reviewDF: DataFrame) {

  def transform: DataFrame = {
    val distinctReviewDF = reviewDF.na.drop().dropDuplicates("review_id", "business_id").na.drop().dropDuplicates(Seq("review_id"))
    distinctReviewDF
  }

  def load: Unit = {
     val tmp=transform.na.drop().dropDuplicates()
    tmp.select(col("review_id"),col("text"),col("date"))
      .write
      .mode(SaveMode.Overwrite)
      .option("createTableColumnTypes","review_id VARCHAR(255)")
      .jdbc(oracleJdbcUrl, "Reviews", oracleConnectionProperties)
  }

  def joinWithBusiness(): DataFrame = {
    // Agréger les données par "business_id"
    val aggregatedReviewDF = transform.groupBy("business_id")
      .agg(
        sum("useful").alias("Reviews_useful_count"),
        sum("cool").alias("Reviews_cool_count"),
        sum("funny").alias("Reviews_funny_count"),
        sum("stars").alias("Reviews_stars_count"),
       )
    // Sélectionner les colonnes pertinentes pour votre table de fait
    val finalAggregatedReviewDF = transform.join(aggregatedReviewDF,Seq("business_id")).select(
      col("business_id"),
      col("review_id"),
      col("Reviews_useful_count"),
      col("Reviews_cool_count"),
      col("Reviews_funny_count"),
      col("Reviews_stars_count"),
     )


    finalAggregatedReviewDF
  }

  def joinedDWBUS(): DataFrame = {
    //Sélectionner uniquement les colonnes "review_id" et "user_id"
    reviewDF.select("review_id", "business_id")
  }
}
