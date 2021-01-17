import org.apache.spark.sql.expressions.Window
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions.{col, monotonically_increasing_id, row_number}
import org.apache.spark.sql.types.IntegerType

object Time {
  def main(args: Array[String]): Unit = {
    val sqlContext = SparkSession.builder()
      .appName("TimeETL")
      .enableHiveSupport()
      .getOrCreate()
    import sqlContext.implicits._
    val (postCodes, crimes1, crimes2, airQuality) = Reader.read(sqlContext)

    crimes1
      .union(crimes2)
      .select("year", "month")
      .distinct().na.drop("all")
      .withColumn("time_id", monotonically_increasing_id())
      .withColumn("time_id", row_number.over(Window.orderBy($"time_id")))
      .withColumn("year", col("year").cast(IntegerType))
      .withColumn("month", col("month").cast(IntegerType))
      .withColumn("quarter", (col("month")/4).cast(IntegerType))
      .select("time_id", "month", "year", "quarter")
      .write.insertInto("time")
  }
}
