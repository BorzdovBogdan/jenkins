import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{broadcast, col, current_date, current_timestamp, date_add, datediff, expr, from_json, from_unixtime, greatest, lit, monotonically_increasing_id, row_number, struct, sum, to_date, to_json, to_timestamp, unix_timestamp, when, window}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{BooleanType, DataTypes, DateType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, RelationalGroupedDataset, SparkSession}

import scala.concurrent.duration.{Duration, MILLISECONDS, SECONDS}

object ElasticSpark {

  case class Expedia(
                      id: java.lang.Long,
                      date_time: String,
                      site_name: Integer,
                      posa_continent: Integer,
                      user_location_country: Integer,
                      user_location_region: Integer,
                      user_location_city: Integer,
                      orig_destination_distance: java.lang.Double,
                      user_id: Integer,
                      is_mobile: Integer,
                      is_package: Integer,
                      channel: Integer,
                      srch_ci: String,
                      srch_co: String,
                      srch_adults_cnt: Integer,
                      srch_children_cnt: Integer,
                      srch_rm_cnt: Integer,
                      srch_destination_id: Integer,
                      srch_destination_type_id: Integer,
                      hotel_id: java.lang.Long)

  case class HotelsWeather(Id: String, Name: String, Country: String, City: String,
                           Geohash: String, wthr_date: String, avg_tmpr_f: Double, avg_tmpr_c: Double)



  def main(args: Array[String]) {
    val hdfsIp = "172.22.57.8"
    val esPort = "9200"
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Streaming_2")
      .config("spark.es.nodes", hdfsIp)
      .config("spark.es.port", esPort)
      .config("es.index.auto.create", "true")
      .config("es.nodes.wan.only", "true")
      .getOrCreate()

    //spark.conf.set("spark.streaming.kafka.consumer.poll.ms", "50000")
    spark.conf.set("es.index.auto.create", "true")

    /*val topic = "kafka_hotels_weather"
    val path_to_expedia = "/data/expedia_4/"
    val schemaHotelsWeather = getSchemaHotelsWeather

    val hotelsWeatherBatch = getBatchDataFromKafka(spark, schemaHotelsWeather, hdfsIp, topic)
    val hotelsWeatherStream = getStreamDataFromKafka(spark, schemaHotelsWeather, hdfsIp, topic)


    val path_2017 = "hdfs://" + hdfsIp + ":9000" + path_to_expedia + "srch_ci_year=2017"
    val path_2016 = "hdfs://" + hdfsIp + ":9000" + path_to_expedia + "srch_ci_year=2016"

    val expediaSchema = getAvroSchema(spark, path_2017)

    val expediaStream = getStream(spark, path_2017, expediaSchema)

    val expediaBatch = getBatch(spark, path_2016, expediaSchema)

    import spark.implicits._
    val expediaStreamDs: Dataset[Expedia] = expediaStream.as[Expedia]
    val expediaBatchDs: Dataset[Expedia] = expediaBatch.as[Expedia]
    val hotelsWeatherBatchDs: Dataset[HotelsWeather] = hotelsWeatherBatch.as[HotelsWeather]
    val hotelsWeatherStreamDs: Dataset[HotelsWeather] = hotelsWeatherStream.as[HotelsWeather]


    val visits2017 = mapVisits(expediaStreamDs, hotelsWeatherStreamDs)
    val visits2016 = mapVisits(expediaBatchDs, hotelsWeatherBatchDs)
    val watermarkedVisits2017 =
      visits2017
        .withColumn("w_timestamp", col("batch_timestamp"))//to_timestamp(current_timestamp().cast("long") - $"batch_timestamp".cast("long") /*% current_timestamp().cast("long")*/))
        .withWatermark("w_timestamp", Duration(1000, MILLISECONDS).toString())
    val updVisits2016 = visits2016.withColumn("w_timestamp", $"batch_timestamp")
    val visitsStats2017 = getVisitsStats(
      watermarkedVisits2017
        .groupBy(
          window(col("w_timestamp"), Duration(2000, MILLISECONDS).toString()).as("w_time"),
          col("hotel_id")
        ), "_2017")
    val visitsStats2016 = getVisitsStats(
      updVisits2016
        .groupBy(
          col("hotel_id")
        ), "_2016")
      .as("init_state")

    val visitsStats2016Upd =
      visitsStats2016
        .withColumnRenamed("hotel_id", "init_hotel_id")*/

    val target_path = "/data/hotels_stays_to_es/"
    val target_path_2 = "/data/hotels_stays_to_es_stream_2/"
    /*visitsStats2016Upd
      .write
      .format("avro")
      .save("hdfs://" + hdfsIp + ":9000" + target_path + "2016")*/
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
    val dated_state = initialState.withColumn("date", col("row_num")*1000*60 + System.currentTimeMillis())
    dated_state
      .write
      .format("avro")
      .save("hdfs://" + hdfsIp + ":9000" + target_path_2 + "2016")
    val streamState = getStream(spark, "hdfs://" + hdfsIp + ":9000" + target_path_2 + "2016", hotelTimeStaysInitSchema("2016"))

    //val dated_state = timed_state.withColumn("date", col("date").cast(DateType))
    //val w_initState = initialState.withColumn("w_timestamp", current_timestamp())
    /*val target_path_2 = "/data/hotels_stays_to_es_2/"
    val topic = "hotels"
    w_initState
      .select(to_json(struct("*")).as("value"))
      .selectExpr("CAST(value AS STRING)")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", hdfsIp + ":31090")
      .option("topic", topic)
      .save()
    val timed_df = getBatchDataFromKafka(spark, hotelTimeStaysInitSchema("2016"), hdfsIp, topic).toDF()*/
    /*w_initState
      .write
      .format("avro")
      .save("hdfs://" + hdfsIp + ":9000" + target_path_2 + "2016")*/

    /*val initialState_2 =
      spark
        //.readStream
        .read
        .format("avro")
        .schema(hotelTimeStaysInitSchema("_2016"))
        .load("hdfs://" + hdfsIp + ":9000" + target_path_2 + "2016").toDF()*/
    /*val visitsToWritten = visitsStats2017
      .join(
        broadcast(initialState),
        visitsStats2017("hotel_id") <=> initialState("init_hotel_id")
      )*/
    //writeToDisk(hdfsIp, target_path, visitsToWritten)

    writeStreamToEs(streamState, "visits")
  }

  private def writeToEs(visToWritten: DataFrame, index: String) = {
    import org.elasticsearch.spark.sql._
    visToWritten.saveToEs(index)
  }

  private def writeStreamToEs(/*host: String,*/ visToWritten: DataFrame, index: String/*, port: String*/) = {
    visToWritten
      .writeStream
      //.outputMode("append")
      .format("es")
      /*.option("es.nodes",host)
      .option("es.port", port)*/
      .option("checkpointLocation","/tmp3/")
      .start(index)
      .awaitTermination()
  }

  private def writeToDisk(hdfsIp: String, target_path: String, visToWritten: DataFrame) = {
    visToWritten
      .writeStream
      .outputMode("append")
      .format("avro")
      .option("truncate", "false")
      .option("checkpointLocation", "hdfs://" + hdfsIp + ":9000" + target_path + "filesink_checkpoint/")
      .option("path", "hdfs://" + hdfsIp + ":9000" + target_path + "2017_2016")
      .trigger(Trigger.ProcessingTime(Duration(30, SECONDS)))
      .start("hdfs://" + hdfsIp + ":9000" + target_path + "2017_2016") //"D:\\Files\\Streaming\\visit")
      .awaitTermination()
  }

  private def hotelTimeStaysInitSchema(year: String): StructType = {
    StructType(Array(
      StructField("date", DataTypes.LongType),
      StructField("init_hotel_id", LongType, nullable = true),
      StructField("erroneous_data_cnt" + year, LongType, nullable = true),
      StructField("short_stay_cnt" + year,LongType, nullable = true),
      StructField("standart_stay_cnt" + year,LongType, nullable = true),
      StructField("standart_extended_stay_cnt"+ year,LongType, nullable = true),
      StructField("long_stay_cnt"+ year, LongType, nullable = true),
      StructField("with_children"+ year, BooleanType, nullable = true),
      StructField("most_popular_stay_type_cnt"+ year, LongType, nullable = true),
      StructField("most_popular_stay_type"+ year, StringType, nullable = true),
      StructField("row_num", IntegerType)
    ))
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

  private def getVisitsStats(visits: RelationalGroupedDataset, year: String) = {
    val structs = Array(
      "erroneous_data_cnt" + year,
      "short_stay_cnt" + year,
      "standart_stay_cnt" + year,
      "standart_extended_stay_cnt" + year,
      "long_stay_cnt" + year
    ).tail.map(
      c => struct(col(c).as("v"), lit(c).as("k"))
    )
    visits
      .agg(
        sum("standart_stay_cnt").as("standart_stay_cnt" + year),
        sum("erroneous_data_cnt").as("erroneous_data_cnt" + year),
        sum("short_stay_cnt").as("short_stay_cnt" + year),
        sum("standart_extended_stay_cnt").as("standart_extended_stay_cnt" + year),
        sum("long_stay_cnt").as("long_stay_cnt" + year),
        sum("srch_children_cnt").as("with_children" + year)
      )
      .withColumn("with_children" + year, col("with_children" + year) > 0)
      .withColumn("most_popular_stay_type_cnt" + year,
        greatest(
          "erroneous_data_cnt" + year,
          "short_stay_cnt" + year,
          "standart_stay_cnt" + year,
          "standart_extended_stay_cnt" + year,
          "long_stay_cnt" + year)
      )
      .withColumn("most_popular_stay_type" + year,
        greatest(
          structs: _*
        )
          .getItem("k")
      )
  }

  private def mapVisits(expediaStreamDs: Dataset[Expedia], hotelsWeatherDs: Dataset[HotelsWeather]) = {
    expediaStreamDs
      .join(
        hotelsWeatherDs,
        expediaStreamDs("hotel_id") === hotelsWeatherDs("id")
          && expediaStreamDs("srch_ci") === hotelsWeatherDs("wthr_date")
      )
      .where(col("avg_tmpr_c") > 0)
      .withColumn(
        "duration",
        datediff(col("srch_co"), col("srch_ci"))
      )
      .select(
        expediaStreamDs.col("*"),
        col("duration"),
        hotelsWeatherDs.col("avg_tmpr_c"),
        hotelsWeatherDs.col("batch_timestamp")
      )
      .withColumn(
        "erroneous_data_cnt",
        when(
          (col("duration") isNull) ||
            (col("duration") > 30) ||
            col("duration") <= 0, 1
        )
          .otherwise(0)
      )
      .withColumn(
        "standart_stay_cnt",
        when(
          (col("duration") >= 2) &&
            col("duration") <= 7, 1
        )
          .otherwise(0)
      )
      .withColumn(
        "standart_extended_stay_cnt",
        when(
          (col("duration") > 7) &&
            col("duration") <= 14, 1
        ).otherwise(0)
      )
      .withColumn(
        "long_stay_cnt",
        when(
          (col("duration") > 14) &&
            col("duration") <= 30, 1
        ).otherwise(0)
      )
      .withColumn(
        "short_stay_cnt",
        when(
          col("duration") === 1, 1
        )
          .otherwise(0)
      )
      .withColumnRenamed("duration", "duration")
  }

  private def getBatch(spark: SparkSession, path: String, expediaSchema: StructType) = {
    spark
      .read
      .schema(expediaSchema)
      .format("avro")
      .load(path)
      .as("init_state")
  }

  private def getSchemaHotelsWeather = {
    DataTypes.createStructType(Array[StructField](
      DataTypes.createStructField("id", DataTypes.StringType, false),
      DataTypes.createStructField("name", DataTypes.StringType, false),
      DataTypes.createStructField("country", DataTypes.StringType, false),
      DataTypes.createStructField("city", DataTypes.StringType, false),
      DataTypes.createStructField("geohash", DataTypes.StringType, false),
      DataTypes.createStructField("wthr_date", DataTypes.StringType, false),
      DataTypes.createStructField("avg_tmpr_f", DataTypes.DoubleType, false),
      DataTypes.createStructField("avg_tmpr_c", DataTypes.DoubleType, false)))
  }

  private def getStreamDataFromKafka(spark: SparkSession, schemaHotelsWeather: StructType, broker: String, topic: String) = {
    val offset = "\"" + topic + "\""
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", broker + ":31090")
      .option("subscribe", topic)
      .option("startingOffsets", """{""" + offset + """:{"0":-2}}""")
      .load()
      //.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      //.as[(String, String)]
      .select(from_json(col("value").cast("string"), schemaHotelsWeather)
        .as("json"), col("timestamp").as("batch_timestamp"))
      .select("json.*", "batch_timestamp")
      .as(Encoders.bean(HotelsWeather.getClass))
  }

  private def getBatchDataFromKafka(spark: SparkSession, schemaHotelsWeather: StructType, broker: String, topic: String) = {
    val offset = "\"" + topic + "\""
    spark
      .read
      //.schema(schemaHotelsWeather)
      .format("kafka")
      .option("kafka.bootstrap.servers", broker + ":31090")
      .option("subscribe", topic)
      .option("startingOffsets", """{"""+offset+""":{"0":-2}}""")
      //.option("endingOffsets", """{"hotels_weather":{"0":500}}""")
      .load()
      //.selectExpr("CAST(value AS STRING) as message")
      .select(from_json(col("value").cast("string"), schemaHotelsWeather)
        .as("json"), col("offset").as("batch_timestamp"))
      //.select(from_json(col("message"), schemaHotelsWeather).as("json"),
      .select("json.*", "batch_timestamp")
      .as(Encoders.bean(schemaHotelsWeather.getClass))
  }


  private def getStream(spark: SparkSession, path: String, expediaSchema: StructType) = {
    spark
      .readStream
      .schema(expediaSchema)
      .format("avro")
      .load(path)
  }

  private def getAvroSchema(spark: SparkSession, path: String) = {
    spark
      .read
      .format("avro")
      .load(path)
      .schema
  }
}