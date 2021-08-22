import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{broadcast, col, current_date, current_timestamp, date_add, datediff, expr, from_json, from_unixtime, greatest, lit, monotonically_increasing_id, row_number, struct, sum, to_date, to_json, to_timestamp, unix_timestamp, when, window}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{BooleanType, DataTypes, DateType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, RelationalGroupedDataset, SparkSession}

object JenkinsSparkDemo {

  def main(args: Array[String]) {
    val hdfsIp = args(0)
    val source_path = args(1) //"/data/hotels_stays_to_es/"
    val target_path = args(2) //"/data/hotels_stays_to_es_stream_3/"
    add_time_series_to_df(hdfsIp, source_path, target_path)
  }

  def add_time_series_to_df(hdfsIp: String, source_path: String, target_path: String): Unit = {

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("jenkins_demo")
      .getOrCreate()

    val windSpec = Window.partitionBy(lit(0))
      .orderBy(monotonically_increasing_id())

    val initialState =
      spark
        .read
        .format("avro")
        .schema(hotelStaysInitSchema("_2016"))
        .load("hdfs://" + hdfsIp + ":9000" + target_path + "2016").toDF()
        .withColumn("row_num", row_number().over(windSpec))
    //val timed_state = initialState.withColumn("date", expr("to_timestamp(date_add(current_date(), row_num*0.001))"))
    val dated_state = initialState.withColumn("date", col("row_num") * 1000 * 60 + System.currentTimeMillis())
    dated_state
      .write
      .format("avro")
      .save("hdfs://" + hdfsIp + ":9000" + source_path + "2016")
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
}