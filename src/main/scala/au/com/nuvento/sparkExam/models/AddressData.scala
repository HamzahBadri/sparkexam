package au.com.nuvento.sparkExam.models

case class AddressData(addressId: String, customerId: String, address: String) {

  /**
   * Parses the AddressData, splitting the address into separate fields
   * @return an Address object with street number, street, city and country
   *         represented individually
   */
  def parseAddresses(): Address = {
    val addressFields = address.split(", ")
    val parsedAddress = Address(addressId, customerId,
      addressFields(0).toInt, addressFields(1),
      addressFields(2), addressFields(3))
    parsedAddress
  }
}
