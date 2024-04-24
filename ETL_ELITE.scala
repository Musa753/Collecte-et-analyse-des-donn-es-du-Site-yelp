import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import java.util.Properties

class ETL_ELITE(  oracleConnectionProperties: Properties, oracleJdbcUrl: String,eliteDF:DataFrame) {

  def extract(): DataFrame = {
    // Lecture des données à partir de PostgreSQL
    var DistincteliteDF = eliteDF.dropDuplicates()
    // Création d'une colonne de clé primaire appelée elite_id
    DistincteliteDF.withColumn("elite_id", monotonically_increasing_id())
  }

  def load(): Unit = {
    extract()
      .drop("user_id")
      .dropDuplicates("year")
      .write
      .mode(SaveMode.Overwrite)
      .jdbc(oracleJdbcUrl, "elite", oracleConnectionProperties)
  }

  def joined(): DataFrame = {
    // Retourne un DataFrame avec seulement les colonnes elite_id,user_id
    extract().select("user_id", "elite_id")
  }
}
