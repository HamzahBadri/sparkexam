package au.com.nuvento.sparkExam

import au.com.nuvento.sparkExam.models.{AccountData, CustomerAccountOutputRow, CustomerData}
import au.com.nuvento.sparkExam.utils.{DictionaryUtils, FileUtils, SparkUtils}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * An object for creating the CustomerAccountOutput dataset
 */
object CustomerAccountOutputCreator {

  /**
   * Appends a new column, "accounts", onto a dataset of CustomerData,
   * which lists all accounts owned by the customer in that row.
   * @param customerDataset A dataset of customers
   * @param lookupAccountUdf A user defined function that maps customers (by their customerId)
   *                         to a sequence of accounts owned by that customer
   * @return A dataframe where each row has a customer's ID, forename, surname
   *         and a sequence of all their accounts
   */
  def matchAccountsToCustomer(customerDataset: Dataset[CustomerData],
                                lookupAccountUdf: UserDefinedFunction): DataFrame = {
    customerDataset.withColumn("accounts", lookupAccountUdf(col("customerId")))
  }

  /**
   * Sorts all accounts in a dataset of AccountData by the customer who owns it,
   * then returns a DataFrame that displays the amount of accounts that customer owns,
   * as well as the total and average balance across all their accounts.
   * @param accountDataset The dataset of accounts
   * @return A dataframe with columns "customerId", "numberAccounts", "totalBalance" and "averageBalance"
   */
  def assembleAccountInfo(accountDataset: Dataset[AccountData]): DataFrame = {
    accountDataset.groupBy("customerId").agg(
      count("customerId").alias("numberAccounts"),
      sum("balance").alias("totalBalance"),
      avg("balance").alias("averageBalance"))
  }

  /**
   * Aggregates a DataFrame of customer information and all accounts they own
   * with a DataFrame containing statistics about those accounts they own,
   * then converts that DataFrame into a Dataset.
   * @param spark The active SparkSession
   * @param customersWithAccounts A DataFrame with columns "customerId", "forename", "surname"
   *                              and "accounts"
   * @param accountInfoDataframe A dataframe with columns "customerId", "numberAccounts",
   *                             "totalBalance" and "averageBalance"
   * @return a Dataset where each row is a CustomerAccountOutputRow
   */
  def assembleCustomerAccountOutput(spark: SparkSession,
                                    customersWithAccounts: DataFrame,
                                    accountInfoDataframe: DataFrame):
  Dataset[CustomerAccountOutputRow] = {
    import spark.implicits._
    customersWithAccounts
      .join(accountInfoDataframe, Seq("customerId"), "left")
      .na.fill(0)
      .as[CustomerAccountOutputRow]
  }

  /**
   * Retrieves a dataset of AccountData rows and a dataset of CustomerData rows,
   * then aggregates them, matching accounts to the appropriate customer.
   * @param spark The active SparkSession
   * @return a Dataset where each row is a CustomerAccountOutputRow
   */
  def createCustomerAccountOutput(spark: SparkSession):
  Dataset[CustomerAccountOutputRow] = {

    val accountPath = FileUtils.accountPath
    val customerPath = FileUtils.customerPath

    import spark.implicits._
    val accountDataFrame = SparkUtils.readDataFrame(spark, accountPath)
    val accountDataset = accountDataFrame.as[AccountData]

    val customerDataFrame = SparkUtils.readDataFrame(spark, customerPath)
    val customerDataset = customerDataFrame.as[CustomerData]

    val customerIds = SparkUtils.getCustomerIds(spark, customerDataset)

    val accountDictionary = DictionaryUtils.getDictionary(customerIds, accountDataset)
    val lookupAccount: String => Seq[AccountData] = (customerId: String) => {
      accountDictionary(customerId)
    }
    val lookupAccountUdf = udf(lookupAccount)
    val customersWithAccounts = matchAccountsToCustomer(customerDataset, lookupAccountUdf)

    val accountInfo = assembleAccountInfo(accountDataset)
    val customerAccountOutput = assembleCustomerAccountOutput(
      spark, customersWithAccounts, accountInfo)

    //customerAccountOutput.show(false)
    customerAccountOutput
  }

}