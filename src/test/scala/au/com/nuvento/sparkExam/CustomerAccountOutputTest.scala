package au.com.nuvento.sparkExam

import au.com.nuvento.sparkExam.models.{AccountData, CustomerAccountOutputRow, CustomerData}
import au.com.nuvento.sparkExam.utils.SparkUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.udf
import org.junit.Assert._
import org.junit._

@Test
class CustomerAccountOutputTest {

  @Test
  def matchAccountsToCustomerTest() = {
    val spark = SparkUtils.createSparkSession()
    import spark.implicits._
    val testData = Seq(("IND0001", "Christopher", "Black"))
    val testDataset = testData.toDF("customerId", "forename", "surname")
      .as[CustomerData]

    val testDictionary = Map("IND0001" -> Seq(AccountData("IND0001","ACC0002",589)))
    val testLookup: String => Seq[AccountData] = (customerId: String) => {
      testDictionary(customerId)
    }
    val testLookupUdf = udf(testLookup)

    val customersWithAccounts = CustomerAccountOutputCreator.matchAccountsToCustomer(
      testDataset, testLookupUdf
    )
    assertTrue(customersWithAccounts.columns.contains("accounts"))
    assertTrue(customersWithAccounts.schema("accounts").dataType.typeName == "array")
    val testOutput = customersWithAccounts.collect()
    //TODO: Resolve issue that testOutput has the "accounts" column as a WrappedArray type, not a Seq
    //assertEquals(testOutput(0)(3), Seq(AccountData("IND0001","ACC0002",589)))
    spark.stop()
  }

  @Test
  def assembleAccountInfoTest() = {
    val spark = SparkUtils.createSparkSession()
    import spark.implicits._
    val testData = Seq(
      ("IND0003", "ACC0001", 686.toLong),
      ("IND0001", "ACC0002", 589.toLong),
      ("IND0003", "ACC0003", 194.toLong))
    val testDataset = testData.toDF("customerId", "accountId", "balance")
      .as[AccountData]
    val accountInfo = CustomerAccountOutputCreator.assembleAccountInfo(testDataset)
    val testOutput = accountInfo.collect()
    assertEquals(testOutput(0), Row("IND0003", 2, 880, 440.0))
    assertEquals(testOutput(1), Row("IND0001", 1, 589, 589.0))
    spark.stop()
  }

  @Test
  def assembleCustomerAccountOutputTest() = {
    val spark = SparkUtils.createSparkSession()
    import spark.implicits._
    val testCustomerData = Seq(
      ("IND0001", "Christopher", "Black", Seq(AccountData("IND0001","ACC0002",589))))
    val testCustomerDataframe = testCustomerData.toDF("customerId", "forename", "surname", "accounts")

    val testAccountData = Seq(
      ("IND0001", 1, 589, 589.0))
    val testAccountDataframe = testAccountData.toDF("customerId", "numberAccounts", "totalBalance", "averageBalance")

    val customerAccountOutput = CustomerAccountOutputCreator.assembleCustomerAccountOutput(
      spark, testCustomerDataframe, testAccountDataframe
    )
    val testOutput = customerAccountOutput.collect()
    assertEquals(testOutput(0), CustomerAccountOutputRow(
      "IND0001", "Christopher", "Black", Seq(AccountData("IND0001","ACC0002",589)),
      1, 589, 589.0))
    spark.stop()
  }

}
