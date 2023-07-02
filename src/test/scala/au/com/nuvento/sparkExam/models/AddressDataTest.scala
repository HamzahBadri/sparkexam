package au.com.nuvento.sparkExam.models

import org.junit.Assert._
import org.junit._

@Test
class AddressDataTest {

  @Test
  def parseAddressTest() = {
    val testAddress = AddressData("ADR001","IND0148","302, De Grassi Street, Toronto, Canada")
    val parsedTestAddress = testAddress.parseAddresses()
    assertEquals(parsedTestAddress.addressId, "ADR001")
    assertEquals(parsedTestAddress.customerId, "IND0148")
    assertEquals(parsedTestAddress.streetNumber, 302)
    assertEquals(parsedTestAddress.street, "De Grassi Street")
    assertEquals(parsedTestAddress.city, "Toronto")
    assertEquals(parsedTestAddress.country, "Canada")
  }

}
