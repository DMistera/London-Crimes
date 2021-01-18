import org.apache.spark.sql.expressions.Window
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions.{col, monotonically_increasing_id, row_number}
import org.apache.spark.sql.types.IntegerType

object Facts {
  def main(args: Array[String]): Unit = {
    val sqlContext = SparkSession.builder()
      .appName("FactsETL")
      .enableHiveSupport()
      .getOrCreate()
    import sqlContext.implicits._
    val (postCodes, crimes1, crimes2, airQuality) = Reader.read(sqlContext)

    val locationsDB = sqlContext.sql("SELECT * FROM location")
    val earningsDB =  sqlContext.sql("SELECT * FROM earnings")
    val timeDB =  sqlContext.sql("SELECT * FROM time")
    val pm10DB =  sqlContext.sql("SELECT * FROM pm10")
    val pm25DB =  sqlContext.sql("SELECT * FROM pm25")
    val typeDB =  sqlContext.sql("SELECT * FROM type_of_crime")
    val unionCrimes = crimes1.union(crimes2)

    unionCrimes
      .select("lsoa_code", "value", "month", "year", "minor_category", "major_category")
      .join(locationsDB, unionCrimes("lsoa_code") === locationsDB("lsoa"))
      .join(postCodes, postCodes("LSOA Code") === locationsDB("lsoa")
        && postCodes("District") === locationsDB("district")
        && postCodes("Ward") === locationsDB("ward")
        && postCodes("Constituency") === locationsDB("constituency")
        && postCodes("Parish") === locationsDB("parish")
        && postCodes("Rural/Urban") === locationsDB("rural")
        && postCodes("Built up area") === locationsDB("built_up_area")
        && postCodes("Water company") === locationsDB("water_company")
      )
      .join(earningsDB, postCodes("Average Income") === earningsDB("average_income"))
      .join(timeDB, unionCrimes("month") === timeDB("month") && unionCrimes("year") === timeDB("year"))
      .join(airQuality, unionCrimes("month") === airQuality("month")  && unionCrimes("year") === airQuality("year"))
      .join(pm10DB, airQuality("pm10").cast(IntegerType) >= pm10DB("range_from") && airQuality("pm10").cast(IntegerType) < pm10DB("range_to"))
      .join(pm25DB, airQuality("pm25").cast(IntegerType) >= pm25DB("range_from") && airQuality("pm25").cast(IntegerType) < pm25DB("range_to"))
      .join(typeDB, unionCrimes("minor_category") === typeDB("minor") && unionCrimes("major_category") === typeDB("major"))
      .select(
        unionCrimes("value"),
        earningsDB("earnings_id") as "earnings_fk",
        locationsDB("location_id") as "location_fk",
        timeDB("time_id") as "time_fk",
        pm10DB("pm10_id") as "pm10_fk",
        pm25DB("pm25_id") as "pm25_fk",
        typeDB("type_id") as "type_fk"
      )
      .withColumn("fact_id", monotonically_increasing_id())
      .withColumn("fact_id", row_number.over(Window.orderBy($"fact_id")))
      .select(col("fact_id"), col("value"), col( "earnings_fk"), col("location_fk"), col("time_fk"), col("pm10_fk"), col("pm25_fk"), col("type_fk"))
      .write.insertInto("facts")
  }
}
