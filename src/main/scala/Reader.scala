import java.text.SimpleDateFormat
import java.util.{Calendar, Locale}

import LondonCrimes.parseAirQualityLine
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions.{col, split}

object Reader {

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

  def read(sqlContext: SparkSession) = {
    import sqlContext.implicits._

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

    (postCodes, crimes1, crimes2, airQuality)
  }
}
