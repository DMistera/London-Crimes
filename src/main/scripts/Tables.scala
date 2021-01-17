spark.sql("""DROP TABLE IF EXISTS `location`""")
spark.sql("""DROP TABLE IF EXISTS `earnings`""")
spark.sql("""DROP TABLE IF EXISTS `time`""")
spark.sql("""DROP TABLE IF EXISTS `pm10`""")
spark.sql("""DROP TABLE IF EXISTS `pm25`""")
spark.sql("""DROP TABLE IF EXISTS `type_of_crime`""")
spark.sql("""DROP TABLE IF EXISTS `facts`""")

spark.sql("""CREATE TABLE `location` (
       `location_id` int,
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
       `water_company` string
       )
      ROW FORMAT SERDE
       'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
      STORED AS INPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
      OUTPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")

spark.sql("""CREATE TABLE `earnings` (
       `earnings_id` int,
       `average_income` int
       )
      ROW FORMAT SERDE
       'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
      STORED AS INPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
      OUTPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")

spark.sql("""CREATE TABLE `time` (
        `time_id` int,
        `month` int,
        `year` int,
        `quarter` int
       )
      ROW FORMAT SERDE
       'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
      STORED AS INPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
      OUTPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")


spark.sql("""CREATE TABLE `pm10` (
        `pm10_id` int,
        `range_from` int,
        `range_to` int,
        `state_description` string
       )
      ROW FORMAT SERDE
       'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
      STORED AS INPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
      OUTPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")

spark.sql("""CREATE TABLE `pm25` (
        `pm25_id` int,
        `range_from` int,
        `range_to` int,
        `state_description` string
       )
      ROW FORMAT SERDE
       'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
      STORED AS INPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
      OUTPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")

spark.sql("""CREATE TABLE `type_of_crime` (
        `type_id` int,
        `minor` string,
        `major` string
       )
      ROW FORMAT SERDE
       'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
      STORED AS INPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
      OUTPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")

spark.sql("""CREATE TABLE `facts` (
        `fact_id` int,
        `value` int,
        `earnings_fk` int,
        `location_fk` int,
        `time_fk` int,
        `pm10_fk` int,
        `pm25_fk` int,
        `type_fk` int
       )
      ROW FORMAT SERDE
       'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
      STORED AS INPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
      OUTPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")

spark.sql(
  """INSERT INTO pm25 VALUES
        (1, 0, 35, 'Low'),
        (2, 35, 53, 'Moderate'),
        (3, 53, 70, 'High'),
        (4, 70, 999, 'Very High');
        """)

spark.sql(
  """INSERT INTO pm10 VALUES
        (1, 0, 50, 'Low'),
        (2, 50, 75, 'Moderate'),
        (3, 75, 100, 'High'),
        (4, 100, 999, 'Very High');
        """)