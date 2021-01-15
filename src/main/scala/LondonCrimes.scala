import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._


object LondonCrimes {
  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf().
      setMaster("local").setAppName("SparkWordCount")
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val postCodes = sqlContext.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("London postcodes.csv").cache()

    val crimes = sqlContext.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("london_crime_by_lsoa.csv").cache()

    val airQuality = sqlContext.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("air-quality-london-monthly-averages.csv").cache()

    sqlContext.sql("""CREATE TABLE `location` (
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

    sqlContext.sql("""CREATE TABLE `time` (
        month: int,
        year: int,
        quarter: int
       )
      ROW FORMAT SERDE
       'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
      STORED AS INPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
      OUTPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")

  }

}
