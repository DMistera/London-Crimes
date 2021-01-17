import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions.{col, monotonically_increasing_id}

object TypeOfCrime {
  def main(args: Array[String]): Unit = {
    val sqlContext = SparkSession.builder()
      .appName("TypeOfCrimeETL")
      .enableHiveSupport()
      .getOrCreate()
    val (postCodes, crimes1, crimes2, airQuality) = Reader.read(sqlContext)

    crimes1
      .union(crimes2)
      .select("major_category","minor_category")
      .distinct()
      .withColumn("type_id", monotonically_increasing_id())
      .withColumn("minor", col("minor_category"))
      .withColumn("major", col("major_category"))
      .write.insertInto("type_of_crime")
  }
}
