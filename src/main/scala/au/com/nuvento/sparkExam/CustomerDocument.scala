package au.com.nuvento.sparkExam

case class CustomerDocument(customerId: String, forename: String, surname: String,
                            accounts: Seq[AccountData], address: Seq[Address])
