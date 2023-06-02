package sparkExamTests

import au.com.nuvento.sparkExam.CustomerAccountOutputCreator
import org.junit.Assert._
import org.junit._

@Test
class CustomerAccountOutputTest {

  def average(sum: Long, total: BigInt): Double  = {
    if (total == 0) 0.0
    else sum.toDouble / total.toDouble
  }

  @Test
  def createOutputTest() = {
    val outputCreator = CustomerAccountOutputCreator
    val customerAccountOutput = outputCreator.createCustomerAccountOutput(
      "src/test/resources/customer_data_test.txt",
      "src/test/resources/account_data_test.txt"
    )
    for (row <- customerAccountOutput.collect()) {
      var sum: Long = 0
      for (account <- row.accounts) {
        assertEquals(row.customerId, account.customerId)
        sum += account.balance
      }
      assertEquals(row.numberAccounts, row.accounts.length)
      assertEquals(row.totalBalance, sum)
      assertTrue(row.averageBalance == average(sum, row.numberAccounts))
    }
  }

}
