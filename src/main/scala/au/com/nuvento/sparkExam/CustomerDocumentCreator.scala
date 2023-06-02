package au.com.nuvento.sparkExam

import com.typesafe.config.ConfigFactory
import org.apache.log4j._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}

object CustomerDocumentCreator {

  def createCustomerDocument(customerAccountOutputPath: String, addressPath: String)
  : Dataset[CustomerDocument] = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("CustomerDocument")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val addressDataset = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(addressPath)
      .as[AddressData]

    val addressParsed = addressDataset.map(row => row.parseAddresses)

    val customerAccountDataset = spark.read.parquet(customerAccountOutputPath)
      .as[CustomerAccountOutput]

    var customerDictionary: Map[String, Seq[Address]] = Map()

    for (customerLine <- customerAccountDataset.collect()) {
      val customerLineId = customerLine.customerId
      val customerAddresses = addressParsed.filter($"customerId" === customerLineId)
      val addressList = customerAddresses.collect().toSeq
      customerDictionary += (customerLineId -> addressList)
    }
    val lookupAddress: String => Seq[Address] = (customerId: String) => {
      customerDictionary(customerId)
    }
    val lookupAddressUdf = udf(lookupAddress)

    val customerDocument = customerAccountDataset
      .select("customerId", "forename", "surname", "accounts")
      .withColumn("address", lookupAddressUdf(col("customerId")))
      .as[CustomerDocument]

    customerDocument.show()
    customerDocument
  }

}