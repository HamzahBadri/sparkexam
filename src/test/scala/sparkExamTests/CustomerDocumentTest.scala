package sparkExamTests

import au.com.nuvento.sparkExam.CustomerDocumentCreator
import org.junit.Assert._
import org.junit._

@Test
class CustomerDocumentTest {

  @Test
  def createDocumentTest() = {
    val documentCreator = CustomerDocumentCreator
    val customerDocument = documentCreator.createCustomerDocument(
      "src/test/resources/CustomerAccountOutputTest",
      "src/test/resources/address_data_test.txt"
    )
    for (row <- customerDocument.collect()) {
      for (address <- row.address) {
        assertEquals(row.customerId, address.customerId)
      }
    }
  }

}
