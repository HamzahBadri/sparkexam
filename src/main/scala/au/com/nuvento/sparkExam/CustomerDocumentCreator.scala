package au.com.nuvento.sparkExam

import au.com.nuvento.sparkExam.models.{Address, AddressData, CustomerAccountOutputRow, CustomerDocumentRow}
import au.com.nuvento.sparkExam.utils.{DictionaryUtils, FileUtils, SparkUtils}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * An object for creating the CustomerDocument
 */
object CustomerDocumentCreator {

  /**
   * Parses all rows in a Dataset of AddressData and
   * splits the String of the "address" column into
   * separate fields.
   * @param spark The active SparkSession
   * @param addressDataset The dataset of AddressData
   * @return A dataset of Addresses
   */
  def parseAddressDataset(spark: SparkSession, addressDataset: Dataset[AddressData]): Dataset[Address] = {
    import spark.implicits._
    addressDataset.map(row => row.parseAddresses)
  }

  /**
   * Append a dataset of CustomerAccountOutputRows with a column
   * containing a Sequence of Addresses lived at by that customer,
   * then converts the new DataFrame into a Dataset.
   *
   * @param spark The active SparkSession
   * @param customerAccountDataset A dataset of CustomerAccountOutputRows
   * @param lookupAddressUdf A user defined function that maps customers (by their customerId)
   *                         to a sequence of addresses lived at by that customer
   * @return A Dataset where each row is a CustomerDocumentRow
   */
  def assembleCustomerDocument(spark: SparkSession,
                               customerAccountDataset: Dataset[CustomerAccountOutputRow],
                               lookupAddressUdf: UserDefinedFunction): Dataset[CustomerDocumentRow] = {
    import spark.implicits._
    val customerDocument = customerAccountDataset
      .select("customerId", "forename", "surname", "accounts")
      .withColumn("address", lookupAddressUdf(col("customerId")))
      .as[CustomerDocumentRow]
    customerDocument
  }

  /**
   * Retrieves a dataset of AddressData rows and a CustomerAccountOutput dataset,
   * parses the addresses into separate fields rather than a single string,
   * then aggregates them, matching address to the appropriate customer.
   * @param spark The active SparkSession
   * @return a Dataset where each row is a CustomerDocumentRow
   */
  def createCustomerDocument(spark: SparkSession):
  Dataset[CustomerDocumentRow] = {

    val addressPath = FileUtils.addressPath
    val customerAccountOutputPath = FileUtils.customerAccountOutputPath

    import spark.implicits._
    val addressDataFrame = SparkUtils.readDataFrame(spark, addressPath)
    val addressDataset = addressDataFrame.as[AddressData]
    val parsedAddressDataset = parseAddressDataset(spark, addressDataset)

    val customerAccountDataset = spark.read.parquet(customerAccountOutputPath)
      .as[CustomerAccountOutputRow]

    val customerIds = SparkUtils.getCustomerIds(spark, customerAccountDataset)

    val addressDictionary = DictionaryUtils.getDictionary(customerIds, parsedAddressDataset)
    val lookupAddress: String => Seq[Address] = (customerId: String) => {
      addressDictionary(customerId)
    }
    val lookupAddressUdf = udf(lookupAddress)

    val customerDocument = assembleCustomerDocument(spark, customerAccountDataset, lookupAddressUdf)

    //customerDocument.show()
    customerDocument
  }

}