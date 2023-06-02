package au.com.nuvento.sparkExam

import com.typesafe.config.ConfigFactory

object CreateBothDocuments {

  def getPath(pathVariable: String): String = {
    val config = ConfigFactory.load("application.conf")
      .getConfig("au.com.nuvento.sparkExam")
    config.getString(pathVariable)
  }

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
