import org.apache.spark.sql.expressions.Window
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions.{col, monotonically_increasing_id, row_number}

object TypeOfCrime {
  def main(args: Array[String]): Unit = {
    val sqlContext = SparkSession.builder()
      .appName("TypeOfCrimeETL")
      .enableHiveSupport()
      .getOrCreate()
    import sqlContext.implicits._
    val (postCodes, crimes1, crimes2, airQuality) = Reader.read(sqlContext)

    crimes1
      .union(crimes2)
      .select("major_category","minor_category")
      .distinct().na.drop("all")
      .withColumn("type_id", monotonically_increasing_id())
      .withColumn("type_id", row_number.over(Window.orderBy($"type_id")))
      .select(col("type_id"), col("minor_category") as "minor", col("major_category") as "major")
      .write.insertInto("type_of_crime")
  }
}
