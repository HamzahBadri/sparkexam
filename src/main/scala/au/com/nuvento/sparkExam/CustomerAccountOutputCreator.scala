package au.com.nuvento.sparkExam

import org.apache.log4j._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

object CustomerAccountOutputCreator {

  def createCustomerAccountOutput(customerPath: String, accountPath: String): Dataset[CustomerAccountOutput] = {

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

    var customerAccountDictionary: Map[String, Seq[AccountData]] = Map()

    for (customerLine <- customerDataset.collect()) {
      val customerLineId = customerLine.customerId
      val customerAccounts = accountDataset.filter($"customerId" === customerLineId)
      val accountList = customerAccounts.collect().toSeq
      customerAccountDictionary += (customerLineId -> accountList)
    }

    val lookupAccount: String => Seq[AccountData] = (customerId: String) => {
      customerAccountDictionary(customerId)
    }
    val lookupAccountUdf = udf(lookupAccount)

    val accountInfoDatabase = accountDataset.groupBy("customerId").agg(
      count("customerId").alias("numberAccounts"),
      sum("balance").alias("totalBalance"),
      avg("balance").alias("averageBalance"))

    val customerAccountOutput = customerDataset
      .withColumn("accounts", lookupAccountUdf(col("customerId")))
      .join(accountInfoDatabase, Seq("customerId"), "left")
      .na.fill(0)
      .as[CustomerAccountOutput]

    customerAccountOutput.show(false)
    customerAccountOutput
    //customerAccountOutput.write.mode("overwrite").parquet(customerAccountOutputPath)
  }

}