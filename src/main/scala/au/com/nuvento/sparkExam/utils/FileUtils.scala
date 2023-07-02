package au.com.nuvento.sparkExam.utils

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{Dataset, SaveMode}

/**
 * An object holding functions and variables relating to data files.
 */
object FileUtils {

  val customerPath = getPath("customerPath")
  val accountPath = getPath("accountPath")
  val addressPath = getPath("addressPath")
  val customerAccountOutputPath = getPath("customerAccountOutputPath")
  val customerDocumentPath = getPath("customerDocumentPath")

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
   * Write a dataset to the data folder as a parquet file.
   * @param path The path of the parquet file
   * @param dataset The dataset to write
   * @tparam T The type of the dataset
   */
  def writeDataset[T](path: String, dataset: Dataset[T]) = {
    dataset.write.mode(SaveMode.Overwrite).parquet(path)
  }

}
