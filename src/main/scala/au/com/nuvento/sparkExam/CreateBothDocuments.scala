package au.com.nuvento.sparkExam

import au.com.nuvento.sparkExam.utils._
import org.apache.log4j.{Level, Logger}

object CreateBothDocuments {

  /**
   * Creates the CustomerAccountOutput dataset and the CustomerDocument dataset,
   * storing them as parquet files
   * @param args
   */
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkUtils.createSparkSession()

    val customerAccountOutput = CustomerAccountOutputCreator
      .createCustomerAccountOutput(spark)
    FileUtils.writeDataset(FileUtils.customerAccountOutputPath, customerAccountOutput)

    val customerDocument = CustomerDocumentCreator
      .createCustomerDocument(spark)
    FileUtils.writeDataset(FileUtils.customerDocumentPath, customerDocument)

    spark.stop()

  }

}
