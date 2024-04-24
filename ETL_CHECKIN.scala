import org.apache.spark
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._

import java.util.Properties

class ETL_CHECKIN(oracleConnectionProperties: Properties, oracleJdbcUrl: String, businessDF: DataFrame, checkinDF: DataFrame) {
  def transform: DataFrame = {
    // Transformer les données de check-in
    val distinctCheckinDF = checkinDF.na.drop().dropDuplicates(Seq("business_id", "date"))
    // Séparation des dates en plusieurs lignes distinctes et conversion en objets Date
    val dateColumns = distinctCheckinDF.select(col("business_id"), explode(split(col("date"), ",")).as("date"))
    val dateDF = dateColumns.withColumn("date", to_date(trim(col("date")), "yyyy-MM-dd")).dropDuplicates(Seq("date"))
    val result = dateDF.withColumn("checkin_id", monotonically_increasing_id())
result
  }

  def load: Unit = {
    // Récupération des données transformées
    val transformedCheckinDF = transform
    // Enregistrement dans la base de données avec les options spécifiées
    transformedCheckinDF
      .select(col("checkin_id")
      ,col("date"))
      .write
      .mode(SaveMode.Overwrite)
      .jdbc(oracleJdbcUrl, "CHECKIN", oracleConnectionProperties)
  }
  def joinWithBusiness: DataFrame = {
    val transformedDF = transform
    val joinedDF = transformedDF
    .select(col("business_id"),col("checkin_id"))
    joinedDF
  }
}
