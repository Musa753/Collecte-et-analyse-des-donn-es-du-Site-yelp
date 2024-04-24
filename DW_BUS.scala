import org.apache.spark.sql.DataFrame

class DW_BUS(reviewDF1: DataFrame, reviewDF2: DataFrame) {

  def joinDWBUS(): DataFrame = {
    reviewDF1.join(reviewDF2, Seq("review_id"), "inner")
   
  }

  def joinedUserFacts(): DataFrame = {
    joinDWBUS().drop("business_id")
  }

  def joinedBusinessFacts(): DataFrame = {
    joinDWBUS().drop("user_id")
  }
}
