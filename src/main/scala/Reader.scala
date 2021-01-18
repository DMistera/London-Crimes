import java.text.SimpleDateFormat
import java.util.{Calendar, Locale}

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions.{col, expr, split}

object Reader {

  def monthToInt(month: String): Int = {
    val date = new SimpleDateFormat("MMM", Locale.ENGLISH).parse(month)
    val cal = Calendar.getInstance
    cal.setTime(date)
    cal.get(Calendar.MONTH)
  }

  def parseAirQualityLine(line: String): String = {
    val pattern = "^(.+) of (.+)-(.+) is (.+) PM10 Particulate: (.+), PM2\\.5 Particulate: (.+), (.+)$".r
    line match {
      case pattern(_, month, year, _, pm10, pm25, _) =>  monthToInt(month).toString + ";" + (year.toInt + 2000).toString + ";" + pm10 + ";" + pm25
      case _ => ""
    }
  }

  def read(sqlContext: SparkSession) = {
    import sqlContext.implicits._

    val postCodes = sqlContext.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("london-postcodes.csv")
      .where("`In Use?` = 'Yes'")
      .where("`Water company` is not null and population is not null and households is not null")
      .groupBy("LSOA Code",  "District"
        , "Ward", "Constituency", "Parish"
        , "Rural/Urban", "Built up area", "Water company"
      )
      .agg(
        expr("round(avg(population)) as Population")
        , expr("round(avg(households)) as Households")
        , expr("round(avg(`Average Income`)) as `Average Income`")
      )

      .cache()

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
      withColumn("split", split($"value", ";")).
      select(
        $"split".getItem(1).as("month").cast("int"),
        $"split".getItem(2).as("year").cast("int"),
        $"split".getItem(3).as("pm10").cast("float"),
        $"split".getItem(4).as("pm25").cast("float")
      ).distinct()

    (postCodes, crimes1, crimes2, airQuality)
  }
}
