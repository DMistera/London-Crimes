import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions.{col, monotonically_increasing_id}

object Earnings {
  def main(args: Array[String]): Unit = {
    val sqlContext = SparkSession.builder()
      .appName("EarningsETL")
      .enableHiveSupport()
      .getOrCreate()
    val (postCodes, crimes1, crimes2, airQuality) = Reader.read(sqlContext)

    postCodes
      .select(col("Average Income") as "average_income")
      .distinct()
      .withColumn("earnings_id", monotonically_increasing_id())
      .write.insertInto("earnings")
  }
}
