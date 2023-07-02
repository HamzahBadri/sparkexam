package au.com.nuvento.sparkExam

import au.com.nuvento.sparkExam.models._
import au.com.nuvento.sparkExam.utils.SparkUtils
import org.apache.spark.sql.functions.udf
import org.junit.Assert._
import org.junit._

@Test
class CustomerDocumentTest {

  @Test
  def parseAddressDatasetTest() = {
    val spark = SparkUtils.createSparkSession()
    import spark.implicits._
    val testData = Seq(
      ("ADR001","IND0001","302, De Grassi Street, Toronto, Canada"),
      ("ADR002","IND0002","344, Oxford Street, London, United Kingdom")
    )
    val testDataset = testData.toDF("addressId", "customerId","address")
      .as[AddressData]
    val parsedTestDataset = CustomerDocumentCreator.parseAddressDataset(spark, testDataset)
    val testOutput = parsedTestDataset.collect()
    assertEquals(testOutput(0),
      Address("ADR001","IND0001",302, "De Grassi Street", "Toronto", "Canada"))
    assertEquals(testOutput(1),
      Address("ADR002","IND0002",344, "Oxford Street", "London", "United Kingdom"))
    spark.stop()
  }

  @Test
  def assembleCustomerDocumentTest() = {
    val spark = SparkUtils.createSparkSession()
    import spark.implicits._
    val testCustomerAccountData = Seq(
      ("IND0001", "Christopher", "Black", Seq(AccountData("IND0001", "ACC0002", 589)),
      1, 589, 589.0))
    val testCustomerDataframe = testCustomerAccountData.toDF(
      "customerId", "forename", "surname", "accounts",
      "numberAccounts", "totalBalance", "averageBalance"
    ).as[CustomerAccountOutputRow]

    val testDictionary = Map("IND0001" -> Seq(
      Address("ADR001","IND0001",302, "De Grassi Street", "Toronto", "Canada")))
    val testLookup: String => Seq[Address] = (customerId: String) => {
      testDictionary(customerId)
    }
    val testLookupUdf = udf(testLookup)

    val customerDocument = CustomerDocumentCreator.assembleCustomerDocument(
      spark, testCustomerDataframe, testLookupUdf
    )
    val testOutput = customerDocument.collect()
    assertEquals(testOutput(0), CustomerDocumentRow(
      "IND0001", "Christopher", "Black", Seq(AccountData("IND0001", "ACC0002", 589)),
      Seq(Address("ADR001","IND0001",302, "De Grassi Street", "Toronto", "Canada"))))
    spark.stop()
  }

}
