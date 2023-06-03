package sparkExamTests

import au.com.nuvento.sparkExam.CreateBothDocuments
import org.junit.Assert._
import org.junit._

@Test
class CreateBothDocumentsTest {

  @Test
  def getPathTest() = {
    assertEquals(CreateBothDocuments.getPath("customerPath"),
      "data/customer_data.txt")
    assertEquals(CreateBothDocuments.getPath("customerAccountOutputPath"),
      "data/CustomerAccountOutput")
    assertEquals(CreateBothDocuments.getPath("accountPath"),
      "data/account_data.txt")
    assertEquals(CreateBothDocuments.getPath("customerDocumentPath"),
      "data/CustomerDocument")
    assertEquals(CreateBothDocuments.getPath("addressPath"),
      "data/address_data.txt")
  }

}
