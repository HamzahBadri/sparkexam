package au.com.nuvento.sparkExam

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CreateCustomerAccountOutput {

  def main(args: Array[String]): Unit = {

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
      .csv("data/account_data.txt")
      .as[AccountData]

    // Load each line of the source data into an Dataset
    val customerDataset = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/customer_data.txt")
      .as[CustomerData]

    var customerAccountDictionary: Map[String, Seq[AccountData]] = Map()

    for (customerLine <- customerDataset.collect()) {
      val customerLineId = customerLine.customerId
      val customerAccounts = accountDataset.filter($"customerId" === customerLineId)
      val accountList = customerAccounts.collect().toSeq
      customerAccountDictionary += (customerLineId -> accountList)
    }

    val customerAccounts = customerDataset.map(row => {
      val accounts = customerAccountDictionary(row.customerId)
      val numberAccounts = accounts.length
      (row.customerId, row.forename, row.surname, accounts, numberAccounts)
    }).toDF("customerId", "forename", "surname", "accounts", "numberAccounts")

    val balanceDatabase = accountDataset.groupBy("customerId").agg(
      sum("balance").alias("totalBalance"),
      avg("balance").alias("averageBalance"))

    val customerAccountOutput = customerAccounts
      .join(balanceDatabase, Seq("customerId"), "left")
      .as[CustomerAccountOutput]

    customerAccountOutput.show(false)
    customerAccountOutput.write.mode("overwrite").parquet("data/CustomerAccountOutput")
  }

}