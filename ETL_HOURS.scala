 import org.apache.spark.sql.{DataFrame, SaveMode}
 import org.apache.spark.sql.functions._
 import java.sql.Date
 import java.util.{  Properties}
 class ETL_HOURS( oracleConnectionProperties: Properties, oracleJdbcUrl: String,businessDF: DataFrame) {

    import org.apache.spark.sql.functions.{col, expr}

    def extract: DataFrame = {
      val daysOfWeek = List("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")

      // Pour chaque jour de la semaine, créez une colonne structurée avec le nom du jour et l'heure correspondante
      val columns = daysOfWeek.map(day => struct(lit(day).alias("day"), col(s"hours.$day").alias("hour")))

      // Créez un tableau contenant toutes les colonnes structurées
      val hours_array = array(columns: _*)

      // Ajoutez le tableau des heures au DataFrame d'origine
      businessDF.withColumn("hours_array", hours_array)
    }
   def transform: DataFrame = {
     val extractedDF = extract

     // Sélectionnez les colonnes business_id et heures à partir du tableau hours_array
     val HoursDF = extractedDF.select(col("business_id"), explode(col("hours_array")).as("hour"))

     // Pivoter les données pour obtenir chaque jour de la semaine en tant que colonne
     val pivotedDF = HoursDF.groupBy("business_id").pivot("hour.day").agg(first("hour.hour"))

     // Renommer les colonnes en minuscules
     val renamedDF = pivotedDF.toDF(pivotedDF.columns.map(_.toLowerCase): _*)

     // Remplacer les valeurs de date nulle par une valeur par défaut
     val defaultDate ="00:00-00:00"
     val cleanedDF = renamedDF.na.fill(defaultDate).dropDuplicates()

     // Ajoutez une colonne hours_id pour identifier de manière unique chaque ligne
     val finalDF = cleanedDF.withColumn("hours_id", monotonically_increasing_id())
     finalDF.show(100)
     // Afficher le DataFrame final
     val tmp=finalDF.na.drop(Seq("hours_id")).dropDuplicates(Seq("hours_id"))
     tmp
   }
    def load : Unit = {
      val finalDF = transform

      // Écrire les données dans la base de données
      finalDF
        .na.drop().dropDuplicates()
        .select("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday", "hours_id")
        .write
        .mode(SaveMode.Overwrite)
        .jdbc(oracleJdbcUrl, "HOURS", oracleConnectionProperties)
    }

    def joinWithBusiness: DataFrame = {
      val transformedDF = transform
      val result=transformedDF.select("business_id","hours_id")
      result.show()
      result
     }
 }