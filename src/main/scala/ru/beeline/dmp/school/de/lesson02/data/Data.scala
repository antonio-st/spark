package ru.beeline.dmp.school.de.lesson02.data

object Data {


	// ctn, report_date, voice_minute, sms_count
	val subscriberHistory = Seq(
		("900123456789", "2023-10-06", 67, 	10),
		("900234567891", "2023-10-05", 34, 	1),
		("900345678912", "2023-10-04", 8,	1),
		("900456789123", "2023-10-05", 195, 560),
		("900567891234", "2023-10-06", 23, 	9),
		("900123456789", "2023-10-03", 76, 6),
		("900234567891", "2023-10-02", 43, 1),
		("900345678912", "2023-10-01", 1, 8),
		("900456789123", "2023-10-02", 519, 56),
		("900567891234", "2023-10-03", 32, 21),
		("900123456789", "2023-10-09", 11, 1),
		("900123456789", "2023-10-08", 23, 2),
		("900567891234", "2023-10-07", 33, 6),
		("900567891234", "2023-10-08", 50, 199),
		("900567891234", "2023-10-09", 2, 4)
	)

	// ctn, market_code, birthdate
	val subscriberInfo = Seq(
		("900123456789", "VIP", "1973-09-01"),
		("900234567891", "NAL", "1989-04-23"),
		("900345678912", "KRD", "2000-02-25"),
		("900456789123", "VIP", "1978-05-06"),
		("900567891234", "KRD", "1992-01-12")
	)

}
