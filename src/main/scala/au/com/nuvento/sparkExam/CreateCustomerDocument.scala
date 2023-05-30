package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object CreateCustomerDocument {

  def main(args: Array[String]): Unit = {

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
      .csv("data/address_data.txt")
      .as[AddressData]

    //val addressParsed = addressDataset.map(row => row.parseAddresses)

    val customerAccountDataset = spark.read.parquet("data/CustomerAccountOutput")
    customerAccountDataset.show(truncate = false)

    /*
    var customerDictionary: Map[String, Seq[Address]] = Map()

    for (customerLine <- customerAccountDataset.collect()) {
      val customerLineId = customerLine.getString(0)
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
    customerDocument.show(truncate = false)
    //customerDocument.write.mode(SaveMode.Overwrite).parquet("data/CustomerDocument")
    */
  }

}