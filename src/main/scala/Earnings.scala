
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions.{col, monotonically_increasing_id, row_number}

object Earnings {
  def main(args: Array[String]): Unit = {
    val sqlContext = SparkSession.builder()
      .appName("EarningsETL")
      .enableHiveSupport()
      .getOrCreate()
    import sqlContext.implicits._
    val (postCodes, crimes1, crimes2, airQuality) = Reader.read(sqlContext)

    postCodes
      .select(col("Average Income") as "average_income")
      .distinct().na.drop("all")
      .withColumn("earnings_id", monotonically_increasing_id())
      .withColumn("earnings_id", row_number.over(Window.orderBy($"earnings_id")))
      .select(col("earnings_id"), col("average_income"))
      .write.insertInto("earnings")
  }
}
