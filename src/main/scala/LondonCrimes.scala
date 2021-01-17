import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.dsl.expressions.{DslExpression, StringToAttributeConversionHelper}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, IntegerType}
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Date
import java.util.Locale


object LondonCrimes {

  def createTables(sqlContext: SQLContext): Unit = {
    sqlContext.sql("""DROP TABLE IF EXISTS `location`""")
    sqlContext.sql("""DROP TABLE IF EXISTS `earnings`""")
    sqlContext.sql("""DROP TABLE IF EXISTS `time`""")
    sqlContext.sql("""DROP TABLE IF EXISTS `pm10`""")
    sqlContext.sql("""DROP TABLE IF EXISTS `pm2.5`""")
    sqlContext.sql("""DROP TABLE IF EXISTS `type_of_crime`""")
    sqlContext.sql("""DROP TABLE IF EXISTS `facts`""")

    sqlContext.sql("""CREATE TABLE `location` (
       'location_id' int,
       `lsoa` string,
       `borough` string,
       `district` string,
       `ward` string,
       `constituency` string,
       `parish` string,
       `rural` string,
       `built_up_area` string,
       `households` int,
       `population` int,
       'water_company' string
       )
      ROW FORMAT SERDE
       'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
      STORED AS INPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
      OUTPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")

    sqlContext.sql("""CREATE TABLE `earnings` (
       'earnings_id' int,
       'average_income' int
       )
      ROW FORMAT SERDE
       'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
      STORED AS INPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
      OUTPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")

    sqlContext.sql("""CREATE TABLE `time` (
        'time_id' int,
        'month' int,
        'year' int,
        'quarter' int
       )
      ROW FORMAT SERDE
       'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
      STORED AS INPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
      OUTPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")


    sqlContext.sql("""CREATE TABLE `pm10` (
        'pm10_id' int,
        'range_from' int,
        'range_to' int,
        'state_description' string
       )
      ROW FORMAT SERDE
       'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
      STORED AS INPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
      OUTPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")

    sqlContext.sql("""CREATE TABLE `pm2.5` (
        'pm25_id' int,
        'range_from' int,
        'range_to' int,
        'state_description' string
       )
      ROW FORMAT SERDE
       'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
      STORED AS INPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
      OUTPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")

    sqlContext.sql("""CREATE TABLE `type_of_crime` (
        'type_id' int,
        'minor' string,
        'major' string
       )
      ROW FORMAT SERDE
       'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
      STORED AS INPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
      OUTPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")

    sqlContext.sql("""CREATE TABLE `facts` (
        'fact_id' int,
        'value' int,
        'earnings_fk' int,
        'location_fk' int,
        'time_fk' int,
        'pm10_fk' int,
        'pm25_fk' int,
        'type_fk' int
       )
      ROW FORMAT SERDE
       'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
      STORED AS INPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
      OUTPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")

    sqlContext.sql(
      """INSERT INTO pm2.5 VALUES
        (0, 35, 'Low'),
        (35, 53, 'Moderate'),
        (53, 70, 'High'),
        (70, 999, 'Very High');
        """)

    sqlContext.sql(
      """INSERT INTO pm10 VALUES
        (0, 50, 'Low'),
        (50, 75, 'Moderate'),
        (75, 100, 'High'),
        (100, 999, 'Very High');
        """)
  }

  def monthToInt(month: String): Int = {
    val date = new SimpleDateFormat("MMM", Locale.ENGLISH).parse(month)
    val cal = Calendar.getInstance
    cal.setTime(date)
    cal.get(Calendar.MONTH)
  }

  def parseAirQualityLine(line: String): String = {
    val pattern = "^(.+) of (.+)-(.+) is (.+) PM10 Particulate: (.+), (.+) PM2.5 Particulate: (.+), (.+)$".r
    line match {
      case pattern(_, month, year, _, pm10, _, pm25, _) =>  monthToInt(month).toString + ";" + (year.toInt + 2000).toString + ";" + pm10 + ";" + pm25
      case _ => ""
    }
  }

  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf().
      setMaster("local").setAppName("SparkWordCount")
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    createTables(sqlContext)

    val postCodes = sqlContext.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("london-postcodes.csv").cache()

    val crimes1 = sqlContext.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("MP-london-crimes.csv").cache()

    val crimes2 = sqlContext.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("CoLP-london-crimes.csv").cache()

    val airQuality = sqlContext.read.textFile("london-air-quality.txt").
      map(line => parseAirQualityLine(line)).
      filter(!_.equals("")).
      withColumn("split", split(col("value"), ";")).
      select(
        col("split").getItem(0).as("month").cast("int"),
        col("split").getItem(1).as("year").cast("int"),
        col("split").getItem(2).as("pm10").cast("float"),
        col("split").getItem(3).as("pm25").cast("float")
      ).distinct()


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

    postCodes
      .select("Average Income")
      .distinct()
      .withColumn("earnings_id", monotonically_increasing_id())
      .withColumn("average_income", col("Average Income"))
      .write.insertInto("earnings")

    crimes1
      .union(crimes2)
      .select("year", "month")
      .distinct()
      .withColumn("time_id", monotonically_increasing_id())
      .withColumn("year", col("year").cast(IntegerType))
      .withColumn("month", col("month").cast(IntegerType))
      .withColumn("quarter", (col("month")/4).cast(IntegerType))
      .write.insertInto("time")

    crimes1
      .union(crimes2)
      .select("major_category","minor_category")
      .distinct()
      .withColumn("type_id", monotonically_increasing_id())
      .withColumn("minor", col("minor_category"))
      .withColumn("major", col("major_category"))
      .write.insertInto("type_of_crime")

    val locationsDB = sqlContext.sql("SELECT * FROM location")
    val earningsDB =  sqlContext.sql("SELECT * FROM earnings")
    val timeDB =  sqlContext.sql("SELECT * FROM time")
    val pm10DB =  sqlContext.sql("SELECT * FROM pm10")
    val pm25DB =  sqlContext.sql("SELECT * FROM pm2.5")
    val typeDB =  sqlContext.sql("SELECT * FROM type_of_crime")
    val unionCrimes = crimes1.union(crimes2)

    unionCrimes
      .select("lsoa_code", "value", "month", "year", "minor_category", "major_category")
      .join(locationsDB, unionCrimes("lsoa_code") === locationsDB("lsoa"))
      .join(postCodes, postCodes("LSOA Code") === locationsDB("lsoa"))
      .join(earningsDB, postCodes("Average Income") === earningsDB("average_income"))
      .join(timeDB, unionCrimes("month") === timeDB("month") && unionCrimes("year") === timeDB("year"))
      .join(airQuality, unionCrimes("month") === airQuality("month")  && unionCrimes("year") === airQuality("year"))
      .join(pm10DB, airQuality("pm10") > pm10DB("range_low") && airQuality("pm10") < pm10DB("range_to"))
      .join(pm25DB, airQuality("pm25") > pm25DB("range_low") && airQuality("pm25") < pm25DB("range_to"))
      .join(typeDB, unionCrimes("minor_category") === typeDB("minor") && unionCrimes("major_category") === typeDB("major"))
      .select(unionCrimes("value"),
        locationsDB("location_id") as "location_fk",
        earningsDB("earnings_id") as "earnings_fk",
        timeDB("time_id") as "time_fk",
        pm10DB("pm10_id") as "pm10_fk",
        pm25DB("pm25_id") as "pm25_fk",
        typeDB("type_id") as "type_fk"
      )
      .withColumn("fact_id", monotonically_increasing_id())
      .write.insertInto("facts")
  }

}
