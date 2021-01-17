import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions.{col, monotonically_increasing_id}
import org.apache.spark.sql.types.IntegerType

object Location {
  def main(args: Array[String]): Unit = {
    val sqlContext = SparkSession.builder()
      .appName("LocationETL")
      .enableHiveSupport()
      .getOrCreate()
    val (postCodes, crimes1, crimes2, airQuality) = Reader.read(sqlContext)

    postCodes
      .join(crimes1, postCodes("LSOA Code") === crimes1("LSOA Code"))
      .join(crimes2, postCodes("LSOA Code") === crimes2("LSOA Code"))
      .select("LSOA Code", "Borough", "District", "Ward", "Constituency", "Parish", "Rural/Urban", "Built up area", "Households", "Population", "Water company")
      .distinct()
      .withColumn("location_id", monotonically_increasing_id())
      .withColumn("lsoa", col("LSOA Code"))
      .withColumn("rural", col("Rural/Urban"))
      .withColumn("households", col("Households").cast(IntegerType))
      .withColumn("population", col("Population").cast(IntegerType))
      .write.insertInto("location")
  }
}
