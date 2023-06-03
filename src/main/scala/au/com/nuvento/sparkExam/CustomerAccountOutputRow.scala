package au.com.nuvento.sparkExam

case class CustomerAccountOutputRow(customerId: String, forename: String, surname: String,
                                    accounts: Seq[AccountData], numberAccounts: BigInt,
                                    totalBalance: Long, averageBalance: Double)
