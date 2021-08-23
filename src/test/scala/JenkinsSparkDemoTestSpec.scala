import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterEach

class JenkinsSparkDemoTestSpec extends AnyFunSuite with BeforeAndAfterEach{

  var spark : SparkSession = _
  val filename = "src/test/resources/testData"
  //JenkinsSparkDemo.add_time_series_to_df()
  // read subset of data for testing
  //val fileContents = Source.fromFile(filename).getLines.mkString
  val testTxtSource = scala.io.Source.fromFile(filename)
  val testConfig = testTxtSource.mkString.split("\n")
  val hdfsIp = testConfig(0)
  val source_path = testConfig(1)
  val target_path = testConfig(2)
  testTxtSource.close()
  override def beforeEach(): Unit = {
    spark = new sql.SparkSession.Builder().appName("test_spark").master("local[*]").getOrCreate()
  }

  test("reading from HDFS") {
    val sparkSession = spark
    val initialDf = JenkinsSparkDemo.readFromHdfs(sparkSession, hdfsIp, source_path)
    initialDf.printSchema()
  }
  test("Check time series") {
    val sparkSession = spark
    val initialDf = JenkinsSparkDemo.readFromHdfs(sparkSession, hdfsIp, source_path)
    val timedDf = JenkinsSparkDemo.addTimeSeriesToDf(initialDf)
    assert(timedDf.columns.contains("date"))
  }

  test("Check written time series") {
    val sparkSession = spark
    val initialDf = JenkinsSparkDemo.readFromHdfs(sparkSession, hdfsIp, source_path)
    val timedDf = JenkinsSparkDemo.addTimeSeriesToDf(initialDf)
    JenkinsSparkDemo.writeToHdfs(hdfsIp, target_path, timedDf)
    val writtenDf = JenkinsSparkDemo.readFromHdfs(sparkSession, hdfsIp, target_path)
    writtenDf.printSchema()
  }

  override def afterEach(): Unit = {
    spark.stop()
  }
}
