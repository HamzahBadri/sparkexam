package au.com.nuvento.sparkExam.utils

import au.com.nuvento.sparkExam.models.CustomerData
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructType}
import org.junit.Assert._
import org.junit._
import scala.collection.JavaConversions._

@Test
class SparkUtilsTest {

  @Test
  def createSparkSessionTest() = {
    val spark = SparkUtils.createSparkSession()
    assertNotNull(spark)
    spark.stop()
  }

  @Test
  def readDataFrameTest() = {
    val spark = SparkUtils.createSparkSession()
    val testDataFrame = SparkUtils.readDataFrame(spark, "src/test/resources/test_dataframe.txt")
    val testDataFrameValues = testDataFrame.collect()
    assertEquals(testDataFrameValues(0).toSeq, Seq("IND0001","Christopher","Black"))
    assertEquals(testDataFrameValues(1).toSeq, Seq("IND0002","Madeleine","Kerr"))
    spark.stop()
  }

  @Test
  def getCustomerIdsTest() = {
    val spark = SparkUtils.createSparkSession()
    import spark.implicits._
    val testData = Seq(
      ("IND0001","Christopher","Black"),
      ("IND0002","Madeleine","Kerr"))
    val testDataset = testData.toDF("customerId", "forename", "surname")
      .as[CustomerData]
    val customerIds = SparkUtils.getCustomerIds(spark, testDataset)
    assertEquals(customerIds, Seq("IND0001", "IND0002"))
    spark.stop()
  }

}
