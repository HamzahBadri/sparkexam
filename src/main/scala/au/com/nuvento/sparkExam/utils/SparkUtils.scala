package au.com.nuvento.sparkExam.utils

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * An object holding various functions relating to Spark functionality.
 */
object SparkUtils {

  /**
   * Creates a SparkSession.
   * @return a new SparkSession
   */
  def createSparkSession(): SparkSession = {
    SparkSession
      .builder
      .appName("sparkExam")
      .master("local[*]")
      .getOrCreate()
  }

  /**
   * A function that creates a DataFrame from data in a file with headers.
   * @param spark the active SparkSession
   * @param path the path to the required data file
   * @return a DataFrame containing the data from the chosen file
   */
  def readDataFrame(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
  }

  /**
   * Retrieve a full list of unique customer identifiers from a dataset.
   * @param spark the active SparkSession
   * @param customerDataset the Dataset of customers, with a column named "customerId"
   * @tparam T the type of rows in the dataset
   * @return A sequence of customer identifier strings
   */
  def getCustomerIds[T](spark: SparkSession, customerDataset: Dataset[T]): Seq[String] = {
    import spark.implicits._
    customerDataset.select("customerId").map(id => id.getString(0))
      .collect().toSeq
  }

}
