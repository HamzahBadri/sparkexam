package au.com.nuvento.sparkExam

import org.apache.log4j._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * An object for creating the CustomerDocument
 */
object CustomerDocumentCreator {

  /**
   * Aggregates a dataset of customers and accounts with a dataset of addresses,
   * parsing addresses into street numbers, streets, cities and countries
   * and matching addresses to their appropriate customer
   * @param customerAccountOutputPath the path to the CustomerAccountOutput parquet file
   * @param addressPath the path to the address data
   * @return a Dataset where each row is a CustomerDocumentRow
   */
  def createCustomerDocument(customerAccountOutputPath: String, addressPath: String):
  Dataset[CustomerDocumentRow] = {

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

    val parsedAddressDataset = addressDataset.map(row => row.parseAddresses)

    val customerAccountDataset = spark.read.parquet(customerAccountOutputPath)
      .as[CustomerAccountOutputRow]

    var addressDictionary: Map[String, Seq[Address]] = Map()

    for (customerLine <- customerAccountDataset.collect()) {
      val customerLineId = customerLine.customerId
      val idAddresses = parsedAddressDataset.filter($"customerId" === customerLineId)
      val idAddressList = idAddresses.collect().toSeq
      addressDictionary += (customerLineId -> idAddressList)
    }
    val lookupAddress: String => Seq[Address] = (customerId: String) => {
      addressDictionary(customerId)
    }
    val lookupAddressUdf = udf(lookupAddress)

    val customerDocument = customerAccountDataset
      .select("customerId", "forename", "surname", "accounts")
      .withColumn("address", lookupAddressUdf(col("customerId")))
      .as[CustomerDocumentRow]

    //customerDocument.show()
    customerDocument
  }

}