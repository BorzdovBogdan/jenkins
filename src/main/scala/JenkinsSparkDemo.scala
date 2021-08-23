import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lit, monotonically_increasing_id, row_number}
import org.apache.spark.sql.types.{BooleanType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object JenkinsSparkDemo {

  def main(args: Array[String]) {
    val hdfsIp = args(0)
    val source_path = args(1) //"/data/hotels_stays_to_es/"
    val target_path = args(2) //"/data/hotels_stays_to_es_stream_3/"
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("jenkins_demo")
      .getOrCreate()
    cleanDir(target_path, spark)
    writeTimeSeries(hdfsIp, source_path, target_path, spark)
  }

  def writeTimeSeries(hdfsIp: String, source_path: String, target_path: String, spark: SparkSession): Unit = {
    val initialState: DataFrame = readFromHdfs(spark, hdfsIp, source_path)
    val dated_state = addTimeSeriesToDf(initialState)
    writeToHdfs(hdfsIp, target_path, dated_state)
  }

  def writeToHdfs(hdfsIp: String, target_path: String, dated_state: DataFrame): Unit = {
    dated_state
      .write
      .format("avro")
      .save("hdfs://" + hdfsIp + ":9000" + target_path + "2016")
  }

  def addTimeSeriesToDf(initialState: DataFrame): DataFrame = {
    val windSpec = Window.partitionBy(lit(0))
      .orderBy(monotonically_increasing_id())
    val state_with_rows = initialState
        .withColumn("row_num", row_number().over(windSpec))
    state_with_rows.withColumn("date", col("row_num") * 1000 * 60 + System.currentTimeMillis())
  }

  def readFromHdfs(spark: SparkSession, hdfsIp: String, source_path: String): DataFrame = {
    val initialState =
      spark
        .read
        .format("avro")
        .schema(hotelStaysInitSchema("_2016"))
        .load("hdfs://" + hdfsIp + ":9000" + source_path + "2016").toDF()
    initialState
  }

  private def hotelStaysInitSchema(year: String): StructType = {
    StructType(Array(
      StructField("init_hotel_id", LongType, nullable = true),
      StructField("erroneous_data_cnt" + year, LongType, nullable = true),
      StructField("short_stay_cnt" + year,LongType, nullable = true),
      StructField("standart_stay_cnt" + year,LongType, nullable = true),
      StructField("standart_extended_stay_cnt"+ year,LongType, nullable = true),
      StructField("long_stay_cnt"+ year, LongType, nullable = true),
      StructField("with_children"+ year, BooleanType, nullable = true),
      StructField("most_popular_stay_type_cnt"+ year, LongType, nullable = true),
      StructField("most_popular_stay_type"+ year, StringType, nullable = true)
    ))
  }

  def cleanDir(path: String, spark: SparkSession): Unit = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val outPutPath = new Path(path)
    if (fs.exists(outPutPath))
      fs.delete(outPutPath, true)
  }
}