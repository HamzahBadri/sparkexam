package au.com.nuvento.sparkExam

import org.apache.log4j._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

/**
 * An object for creating the CustomerAccountOutput dataset
 */
object CustomerAccountOutputCreator {

  /**
   * Aggregates a dataset of customers with a dataset of accounts,
   * matching accounts to their appropriate customer.
   * @param customerPath the path to the customer data
   * @param accountPath the path to the account data
   * @return a Dataset where each row is a CustomerAccountOutputRow
   */
  def createCustomerAccountOutput(customerPath: String, accountPath: String):
  Dataset[CustomerAccountOutputRow] = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("CustomerAccountOutput")
      .master("local[*]")
      .getOrCreate()

    // Load each line of the source data into an Dataset
    import spark.implicits._
    val accountDataset = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(accountPath)
      .as[AccountData]

    // Load each line of the source data into an Dataset
    val customerDataset = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(customerPath)
      .as[CustomerData]

    var accountDictionary: Map[String, Seq[AccountData]] = Map()

    for (customerLine <- customerDataset.collect()) {
      val customerLineId = customerLine.customerId
      val idAccounts = accountDataset.filter($"customerId" === customerLineId)
      val idAccountList = idAccounts.collect().toSeq
      accountDictionary += (customerLineId -> idAccountList)
    }

    val lookupAccount: String => Seq[AccountData] = (customerId: String) => {
      accountDictionary(customerId)
    }
    val lookupAccountUdf = udf(lookupAccount)

    val accountInfoDataframe = accountDataset.groupBy("customerId").agg(
      count("customerId").alias("numberAccounts"),
      sum("balance").alias("totalBalance"),
      avg("balance").alias("averageBalance"))

    val customerAccountOutput = customerDataset
      .withColumn("accounts", lookupAccountUdf(col("customerId")))
      .join(accountInfoDataframe, Seq("customerId"), "left")
      .na.fill(0)
      .as[CustomerAccountOutputRow]

    //customerAccountOutput.show(false)
    customerAccountOutput
  }

}