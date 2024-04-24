import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.jdbc.JdbcDialects

object Main {
  def main(args: Array[String]): Unit = {
    // Initialiser SparkSession
    val spark = SparkSession.builder()
      .appName("ETL_JOBS")
      .config("spark.master", "local")
      .getOrCreate()
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    try {
      // Chargement des données depuis les fichiers CSV et JSON
      val tipDF = spark.read.format("csv")
        .option("header", "true")
        .load("/home1/mt244221/Desktop/data/yelp_academic_dataset_tip.csv")
        .select(
          col("text"),
          col("date"),
          col("compliment_count"),
          col("business_id"),
          col("user_id")
        )

      val checkinDF = spark.read.json("/home1/mt244221/Desktop/data/yelp_academic_dataset_checkin.json")
      val businessDF = spark.read.json("/home1/mt244221/Desktop/data/yelp_academic_dataset_business.json")

      // Informations de connexion à la base de données PostgreSQL
      val postgresHost = "stendhal.iem"
      val postgresPort = "5432"
      val postgresDatabase = "tpid2020"
      val postgresUser = "tpid"
      val postgresPassword = "tpid"

      // Créer l'URL JDBC pour PostgreSQL
      val postgresJdbcUrl = s"jdbc:postgresql://$postgresHost:$postgresPort/$postgresDatabase"

      // Propriétés de connexion pour PostgreSQL
      val postgresConnectionProperties = new java.util.Properties()
      postgresConnectionProperties.setProperty("user", postgresUser)
      postgresConnectionProperties.setProperty("password", postgresPassword)

      // Informations de connexion à la base de données Oracle
      val oracleHost = "stendhal.iem"
      val oraclePort = "1521"
      val oracleDatabase = "enss2023"
      val oracleUser = "mt244221"
      val oraclePassword = "mt244221"

      // Créer l'URL JDBC pour Oracle
      val oracleJdbcUrl = s"jdbc:oracle:thin:@$oracleHost:$oraclePort/$oracleDatabase"

      // Propriétés de connexion pour Oracle
      val oracleConnectionProperties = new java.util.Properties()
      oracleConnectionProperties.setProperty("user", oracleUser)
      oracleConnectionProperties.setProperty("password", oraclePassword)

      // Chargement des données depuis les tables PostgreSQL
      val reviewDF = spark.read
        .option("partitionColumn", "spark_partition") // Colonne sur laquelle partitionner
        .option("lowerBound", "0") // Valeur de la borne inférieure de la colonne partitionnée
        .option("upperBound", "99") // Valeur de la borne supérieure de la colonne partitionnée
        .option("numPartitions", "100") // Nombre de partitions souhaité
        .jdbc(postgresJdbcUrl, "yelp.review", postgresConnectionProperties)



      val eliteDF = spark.read.jdbc(postgresJdbcUrl, "yelp.elite", postgresConnectionProperties)
      val userDF = spark.read.jdbc(postgresJdbcUrl, "yelp.user", postgresConnectionProperties)

       //instances de classes D1
      val categoriesETL = new ETL_CATEGORIES(oracleConnectionProperties, oracleJdbcUrl, businessDF)
      val checkinETL = new ETL_CHECKIN(oracleConnectionProperties, oracleJdbcUrl, businessDF, checkinDF)
      val hoursETL = new ETL_HOURS(oracleConnectionProperties, oracleJdbcUrl, businessDF)
      val tipETL = new ETL_TIP(oracleConnectionProperties, oracleJdbcUrl, businessDF, tipDF)
      val locationETL = new ETL_LOCATIONS(oracleConnectionProperties, oracleJdbcUrl, businessDF)
      val reviewETL_Business = new ETL_REVIEWS(oracleConnectionProperties, oracleJdbcUrl, businessDF, reviewDF)
      val reviewETL_user = new ETL_REVIEWS2(oracleConnectionProperties, oracleJdbcUrl,  reviewDF)

      //instances de classes D2
      val usersETL = new ETL_USERS(oracleConnectionProperties, oracleJdbcUrl, businessDF, reviewDF, userDF)
      val eliteETL = new ETL_ELITE(oracleConnectionProperties, oracleJdbcUrl, eliteDF)
      val timeETL = new ETL_TIME(oracleConnectionProperties, oracleJdbcUrl, userDF)
 
      //Recherche de coherences entre les dimensions des deux datamarts
      val ReviewjoinedDWBUS = reviewETL_Business.joinedDWBUS()
      val ReviewjoinedDWBUS2 = reviewETL_user.joinedDWBUS()

      //inialisations de la classe qui s'occupe de la recherche de cette intersection 
      //et qui à des methodes pour retourner directements les DF pour les jointures avec 
      //les tables de faits respectives 

      val dwBus = new DW_BUS(ReviewjoinedDWBUS, ReviewjoinedDWBUS2)

      val tmp=dwBus.joinedUserFacts()
      //Resultat jointures 
      val userReviews=tmp.join(reviewETL_user.joined(),Seq("review_id"), "inner")
      val BusinessReviews=tmp.join(reviewETL_Business.joinWithBusiness(),Seq("review_id"), "inner")

   //chargement des dimensions du datamart D2 
      //il y'aura qu'une seule dimension review donc il sera ajouter lors du chagement
      //de l'autre dimension car le type du text doit etre caster de varchar en CLOB
      //et pour ne pas modifie le type des autres colonnes on le fera à la fin
      eliteETL.load()
      timeETL.load()
   
    // Construction de la table de fait  user 
    val joinedDF = userReviews
        .join(eliteETL.joined(), Seq("user_id"), "inner")
        .join(timeETL.joined(), Seq("user_id"), "inner")
        .join(userDF.select("user_id","fans","average_stars","name","review_count"), Seq("user_id"), "inner")
      
      val joinedDF2 = joinedDF.na.drop()
      val finalDF = joinedDF2.dropDuplicates()

      finalDF.write
      .mode(SaveMode.Overwrite)
      .jdbc(oracleJdbcUrl, "FACT_USERS", oracleConnectionProperties)
 

//chargement des dimensions dans le dw :
    checkinETL.load
    hoursETL.load
    locationETL.load

//jointure pour avoir la table de fait business
    val joinedCategoriesDF = categoriesETL.joinWithBusiness
    val joinedCheckinDF = checkinETL.joinWithBusiness
    val joinedHoursDF = hoursETL.joinWithBusiness
    val joinedTipDF = tipETL.joinWithBusiness
    val joinedLocationDF = locationETL.joinWithBusiness
       
  val joinedDFBusiness = joinedCategoriesDF
  .join(joinedCheckinDF, Seq("business_id"), "inner")
  .join(joinedHoursDF, Seq("business_id"), "inner")
  .join(joinedTipDF, Seq("business_id"), "inner")
  .join(joinedLocationDF, Seq("business_id"), "inner")
  .join(BusinessReviews, Seq("business_id"), "inner")

 
    val joinedDFBusiness1 = joinedDFBusiness.na.fill(0)
    val joinedDFBusiness2 = joinedDFBusiness1.na.drop()

    joinedDFBusiness2.write
      .mode(SaveMode.Overwrite)
      .jdbc(oracleJdbcUrl, "FACT_BUSINESS", oracleConnectionProperties)

    
//chargement des tables avec des colonnes en CLOB 
    val dialect = new OracleDialect
    JdbcDialects.registerDialect(dialect)
    tipETL.load
    reviewETL_user.load(userReviews)

    
    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally {
      spark.stop()
    } 

  } 
  }
