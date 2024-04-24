import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import java.util.Properties

class ETL_TIP(oracleConnectionProperties: Properties, oracleJdbcUrl: String, businessDF: DataFrame, tipDF: DataFrame) {

  def transform: DataFrame = {
     val distinctTipDF = tipDF.dropDuplicates(Seq("business_id", "compliment_count", "date", "text"))
    val tipWithIDDF = distinctTipDF.na.drop().withColumn("tip_id", monotonically_increasing_id())
     tipWithIDDF.na.drop().dropDuplicates(Seq("tip_id"))
  }

  def load: Unit = {
     val tmp=transform.na.drop(Seq("tip_id")).dropDuplicates(Seq("tip_id"))
    tmp
      .withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
      .select("tip_id","text","date")
      .write
      .mode(SaveMode.Overwrite)
      .jdbc(oracleJdbcUrl, "TIP", oracleConnectionProperties)
  }
  def joinWithBusiness: DataFrame = {

    val sumComplimentDF = transform.groupBy("business_id")
      .agg(sum("compliment_count").alias("TIP_compliments_count")).withColumn("tip_id", monotonically_increasing_id())



    val joinedBusinessTipDF = sumComplimentDF
     sumComplimentDF

  }
}
