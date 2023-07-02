package au.com.nuvento.sparkExam.models

case class CustomerAccountOutputRow(customerId: String, forename: String, surname: String,
                                    accounts: Seq[AccountData], numberAccounts: BigInt,
                                    totalBalance: Long, averageBalance: Double)
