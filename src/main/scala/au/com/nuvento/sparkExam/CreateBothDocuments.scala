package au.com.nuvento.sparkExam

import com.typesafe.config.ConfigFactory

object CreateBothDocuments {

  /**
   * Extracts the path for input and output data from the config file
   * @param pathVariable the variable detailing the data to retrieve
   * @return a String with the path to the desired data
   */
  def getPath(pathVariable: String): String = {
    val config = ConfigFactory.load("application.conf")
      .getConfig("au.com.nuvento.sparkExam")
    config.getString(pathVariable)
  }

  /**
   * Creates the CustomerAccountOutput dataset and the CustomerDocument dataset,
   * storing them as parquet files
   * @param args
   */
  def main(args: Array[String]): Unit = {

    val customerPath = getPath("customerPath")
    val accountPath = getPath("accountPath")
    val addressPath = getPath("addressPath")
    val customerAccountOutputPath = getPath("customerAccountOutputPath")
    val customerDocumentPath = getPath("customerDocumentPath")

    val customerAccountOutput = CustomerAccountOutputCreator
      .createCustomerAccountOutput(customerPath, accountPath)
    customerAccountOutput.write.mode("overwrite").parquet(customerAccountOutputPath)

    val customerDocument = CustomerDocumentCreator
      .createCustomerDocument(customerAccountOutputPath, addressPath)
    customerDocument.write.mode("overwrite").parquet(customerDocumentPath)

  }

}
