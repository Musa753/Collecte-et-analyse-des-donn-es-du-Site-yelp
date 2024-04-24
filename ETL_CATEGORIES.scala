import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import java.util.Properties

class ETL_CATEGORIES( oracleConnectionProperties: Properties, oracleJdbcUrl: String,businessDF: DataFrame) {
  def extract: DataFrame = {
    businessDF.select(col("business_id"), explode(split(col("categories"), ", ")).as("category"))
  }

  def transform: DataFrame = {
    extract.na.drop().dropDuplicates(Seq("business_id","category"))
      .withColumn("category_id", monotonically_increasing_id())
  }

  def load: Unit = {
    transform
      .write
      .option("createTableColumnTypes", "category_id INTEGER, category VARCHAR(255)")
      .mode(SaveMode.Overwrite)
      .jdbc(oracleJdbcUrl, "CATEGORIES", oracleConnectionProperties)
  }
  def joinWithBusiness: DataFrame = {
  val tmp=transform.select("business_id","category_id")

    tmp.show()
    tmp
  }


}