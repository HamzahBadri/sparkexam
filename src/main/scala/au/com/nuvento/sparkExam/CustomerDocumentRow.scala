package au.com.nuvento.sparkExam

case class CustomerDocumentRow(customerId: String, forename: String, surname: String,
                               accounts: Seq[AccountData], address: Seq[Address])
