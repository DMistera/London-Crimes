import Reader.monthToInt

object Test {

  def parseAirQualityLine(line: String): String = {
    val pattern = "^(.+) of (.+)-(.+) is (.+) PM10 Particulate: (.+), PM2\\.5 Particulate: (.+), (.+)$".r
    line match {
      case pattern(_, month, year, _, pm10, pm25, _) =>  monthToInt(month).toString + ";" + (year.toInt + 2000).toString + ";" + pm10 + ";" + pm25
      case _ => ""
    }
  }

  def main(args: Array[String]): Unit = {
    val line = parseAirQualityLine("Air quality in the month of Nov-11 is as follows (ug/m3). London Mean Roadside: [Nitric Oxide: 122.8, Nitrogen Dioxide: 48.8, Oxides of Nitrogen: 186.7, Ozone: 10.9, PM10 Particulate: 35.4, PM2.5 Particulate: 25.2, Sulphur Dioxide: 3.4]; London Mean Background: [Nitric Oxide: 45.4, Nitrogen Dioxide: 42.6, Oxides of Nitrogen: 87.9, Ozone: 17.3, PM10 Particulate: 27.8, PM2.5 Particulate: 23.5, Sulphur Dioxide: 5.1]")
    print(line)
  }
}
