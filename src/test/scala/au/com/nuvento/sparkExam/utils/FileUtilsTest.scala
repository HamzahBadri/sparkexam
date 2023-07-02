package au.com.nuvento.sparkExam.utils

import au.com.nuvento.sparkExam.models.CustomerData
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructType}
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConversions._
import scala.reflect.io.File

class FileUtilsTest {

  @Test
  def getPathTest() = {
    assertEquals(FileUtils.getPath("customerPath"),
      "data/customer_data.txt")
    assertEquals(FileUtils.getPath("customerAccountOutputPath"),
      "data/CustomerAccountOutput")
    assertEquals(FileUtils.getPath("accountPath"),
      "data/account_data.txt")
    assertEquals(FileUtils.getPath("customerDocumentPath"),
      "data/CustomerDocument")
    assertEquals(FileUtils.getPath("addressPath"),
      "data/address_data.txt")
  }

  @Test
  def writeDatasetTest() = {
    File("src/test/resources/testDataset.txt").delete()
    val spark = SparkUtils.createSparkSession()
    import spark.implicits._
    val testData = Seq(
      ("IND0001", "Christopher", "Black"),
      ("IND0002", "Madeleine", "Kerr"))
    val testDataset = testData.toDF("customerId", "forename", "surname")
      .as[CustomerData]
    FileUtils.writeDataset("src/test/resources/testDataset", testDataset)
    assertTrue(File("src/test/resources/testDataset").exists)
    spark.stop()
  }

}
