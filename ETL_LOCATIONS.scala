import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import java.util.Properties

class ETL_LOCATIONS( oracleConnectionProperties: Properties, oracleJdbcUrl: String,businessDF: DataFrame) {

  def extract: DataFrame = {
    businessDF.select(
      col("business_id"),
      col("address").as("location"),
      col("city"),
      col("name").as("business_name"),
      col("state"),
      col("postal_code").as("postal_code"),
      col("latitude"),
      col("longitude")
    )
  }

  def transform: DataFrame = {
  val  tmp=extract.dropDuplicates(Seq("location", "city", "state", "postal_code", "latitude", "longitude"))
      tmp.withColumn("location_id", monotonically_increasing_id())
  }

  def load: Unit = {
     val tmp=transform.drop("business_id").na.drop(Seq("location_id")).dropDuplicates()
    tmp.write
       .mode(SaveMode.Overwrite)
      .jdbc(oracleJdbcUrl, "LOCATION", oracleConnectionProperties)
  }

  def joinWithBusiness: DataFrame = {
    val transformedDF = transform
    val result = transformedDF.select("business_id","business_name","location_id")
    result
  }
}
