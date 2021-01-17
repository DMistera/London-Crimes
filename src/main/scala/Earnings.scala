import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.{col, monotonically_increasing_id}

object Earnings {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().
      setMaster("local").setAppName("SparkWordCount")
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val (postCodes, crimes1, crimes2, airQuality) = Reader.read(sqlContext)

    postCodes
      .select(col("Average Income") as "average_income")
      .distinct()
      .withColumn("earnings_id", monotonically_increasing_id())
      .write.insertInto("earnings")
  }
}
