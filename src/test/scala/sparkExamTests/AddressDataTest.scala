package sparkExamTests

import org.junit._
import Assert._
import au.com.nuvento.sparkExam.AddressData

@Test
class AddressDataTest {

  @Test
  def parseAddressTest() = {
    val testAddress = AddressData("ADR001","IND0148","302, De Grassi Street, Toronto, Canada")
    val parsedTestAddress = testAddress.parseAddresses()
    assertEquals(parsedTestAddress.addressId, testAddress.addressId)
    assertEquals(parsedTestAddress.customerId, testAddress.customerId)
    assertEquals(parsedTestAddress.streetNumber, 302)
    assertEquals(parsedTestAddress.street, "De Grassi Street")
    assertEquals(parsedTestAddress.city, "Toronto")
    assertEquals(parsedTestAddress.country, "Canada")
  }

}
