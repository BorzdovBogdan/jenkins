import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils.IO
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.mockito.ArgumentMatchers._
import org.mockito.{ArgumentCaptor, MockitoSugar}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TestSpec extends AnyFlatSpec with Matchers with MockitoSugar {
  val spark = SparkSession.builder
    .master("local[*]")
    .getOrCreate()

  // read subset of data for testing
  val testData = spark.read.json("src/test/resources/testData.json")

  "My  spark aggregations" should "work" in {
    withObjectMocked[IO.type] {
      // here we will capture results of transformations
      val dataFrameCaptor = ArgumentCaptor.forClass(classOf[DataFrame])

      // instead of actually reading data using loadDataFromBucket method
      // we will swap it with our testing subset
      when(IO.loadDataFromBucket(anyString())
        .thenReturn(testData)
        // with void method you can use doNothing
        //doNothing
        .when(IO)
        .writeResultsToBucket(any[DataFrame], anyString())

      //
      runSparkTransformations()
      //

      // now to capture results of the transformations we will use verify
      verify(IO)
        .writeResultsToBuckets(
          dataFrameCaptor.capture(),
          anyString()
        )

      val result: DataFrame = dataFrameCaptor.getValue
      // the result value  above is your actual result
      // now you can write your assertions
      // to compare it with expected results

      // examples:

      // assert expected columns
      result.columns should contain theSameElementsAs Seq(
        "foo",
        "bar"
      )

      // assert expected elements in the "foo" column
      val foo = result.select("foo").collect.map(_.getString(0))
      foo should contain theSameElementsAs Seq("foo1", "foo2", "foo3")

    }
  }
}
