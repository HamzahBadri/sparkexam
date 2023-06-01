package au.com.nuvento.sparkExam

case class AddressData(addressId: String, customerId: String, address: String) {
  def parseAddresses(): Address = {

    val addressFields = address.split(", ")
    val parsedAddress = Address(addressId, customerId,
      addressFields(0).toInt, addressFields(1),
      addressFields(2), addressFields(3))
    parsedAddress
  }
}
