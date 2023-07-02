package au.com.nuvento.sparkExam.utils

import au.com.nuvento.sparkExam.models.AccountData
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructType}
import org.junit.Assert._
import org.junit._
import scala.collection.JavaConversions._

class DictionaryUtilsTest {

  @Test
  def getDictionaryTest() = {
    val spark = SparkUtils.createSparkSession()
    import spark.implicits._
    val testData = Seq(
      ("IND0003", "ACC0001", 687.toLong),
      ("IND0001", "ACC0002", 589.toLong),
      ("IND0003", "ACC0003", 194.toLong))
    val testDataset = testData.toDF("customerId", "accountId", "balance")
      .as[AccountData]
    val testCustomerIds = Seq("IND0001", "IND0002", "IND0003")
    val testDictionary = DictionaryUtils.getDictionary(testCustomerIds, testDataset)
    assertEquals(testDictionary("IND0001"), Seq(AccountData("IND0001", "ACC0002", 589)))
    assertEquals(testDictionary("IND0002"), Seq())
    assertEquals(testDictionary("IND0003"), Seq(
      AccountData("IND0003", "ACC0001", 687),
      AccountData("IND0003", "ACC0003", 194)))
    spark.stop()
  }

}
