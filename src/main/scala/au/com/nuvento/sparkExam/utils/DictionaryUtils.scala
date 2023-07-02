package au.com.nuvento.sparkExam.utils

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col

/**
 * An object for creating dictionaries, used to create user defined functions
 * that map identifiers to objects.
 */
object DictionaryUtils {

  /**
   * Create a dictionary that maps customer identifiers to each
   * element in a dataset that has that identifier
   * @param customerIds A sequence of customer identifier strings
   * @param dataset A dataset of elements, one of which is a customer identifier
   * @tparam T The type of the dataset
   * @return A map of Strings to sequences of T,
   *         where each T shares the customer identifier key
   */
  def getDictionary[T](customerIds: Seq[String],
                              dataset: Dataset[T]): Map[String, Seq[T]] = {
    var dictionary: Map[String, Seq[T]] = Map()
    for (lineCustomerId <- customerIds) {
      val idAccounts = dataset.filter(col("customerId").contains(lineCustomerId))
      val idAccountList = idAccounts.collect().toSeq
      dictionary += (lineCustomerId -> idAccountList)
    }
    dictionary
  }

}
