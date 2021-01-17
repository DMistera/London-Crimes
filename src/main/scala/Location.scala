import org.apache.spark.sql.expressions.Window
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions.{col, monotonically_increasing_id, row_number}
import org.apache.spark.sql.types.IntegerType

object Location {
  def main(args: Array[String]): Unit = {
    val sqlContext = SparkSession.builder()
      .appName("LocationETL")
      .enableHiveSupport()
      .getOrCreate()
    import sqlContext.implicits._
    val (postCodes, crimes1, crimes2, airQuality) = Reader.read(sqlContext)

    val unionCrimes = crimes1.union(crimes2)

    postCodes
      .join(unionCrimes, postCodes("LSOA Code") === unionCrimes("lsoa_code"))
      .select(col("LSOA Code"), unionCrimes("Borough") as "Borough", col("District"), col("Ward"), col("Constituency"), col("Parish"), col("Rural/Urban"), col("Built up area"), col("Households"), col("Population"), col("Water company"))
      .distinct().na.drop("all")
      .withColumn("location_id", monotonically_increasing_id())
      .withColumn("location_id", row_number.over(Window.orderBy($"location_id")))
      .withColumn("households", col("Households").cast(IntegerType))
      .withColumn("population", col("Population").cast(IntegerType))
      .select(
        col("location_id"),
        col("LSOA Code") as "lsoa",
        col("Borough"),
        col("District"),
        col("Ward") as "ward",
        col("Constituency"),
        col("Parish"),
        col("Rural/Urban") as "rural",
        col("Built up area") as "built_up_area",
        col("households"),
        col("population"),
        col("Water company") as "water_company"
      )
      .write.insertInto("location")
  }
}
