import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object Tables {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().
      setMaster("local").setAppName("SparkWordCount")
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
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
}
