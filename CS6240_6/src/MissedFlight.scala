import java.text.SimpleDateFormat
import java.util.Date
import java.text.ParseException
import java.util.ArrayList

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD


/**
 * Spark program to calculate Missed Connections
 * 
 * @author Adib Alwani
 */
 object MissedFlight {

 	case class Pair (
 		CRSTime : Long,
 		time : Long
 	)

 	def main(args: Array[String]) {
 		val startTime: Long = System.currentTimeMillis

 		val conf = new SparkConf().setAppName("MissedFlight").setMaster("local")
 		val sc = new SparkContext(conf)

    	// Input records in rows-array
    	val rows = sc.textFile("all")
    		.map ( row => parse(row.toString) )
    		.filter ( row => isValidRecord(row) && sanityTest(row) && !isCancelled(row) )
    		.map ( row => { convertToFlight(row) } )
    
    	val depRows = rows.map ( row => { (row._1, row._3) } )
    	val arrRows = rows.map ( row => { (row._2, row._4) } )
    
    	val joinedRows = depRows.cogroup(arrRows)
    
    	val result = joinedRows.map ( row => {
    		val (k, v) = row
    		val (depIterable, arrIterable) = v
    		var depList = new ArrayList[Pair]()
    		var arrList = new ArrayList[Pair]()
    		var connection : Int = 0
    		var missedConnection : Int = 0

        // Fill List
        depIterable.foreach ( item => {
      		depList.add(Pair(item._1, item._2))
      	})
        arrIterable.foreach ( item => {
      		arrList.add(Pair(item._1, item._2))
      	})
      
        // Find Connections
        val arrIterator = arrList.iterator
        while (arrIterator.hasNext()) {
      		val arrPair = arrIterator.next()
      		val CRSArrTime = arrPair.CRSTime
      		val arrTime = arrPair.time

      		val depIterator = depList.iterator
      		while (depIterator.hasNext()) {
      			val depPair = depIterator.next()
      			val CRSDepTime = depPair.CRSTime
      			val depTime = depPair.time

      			if (isConnection(CRSArrTime, CRSDepTime)) {
      				connection += 1;
      				if (isMissedConnection(arrTime, depTime)) {
      					missedConnection += 1;
					}
      			}
      		}
        }

		// Emit
		k._1 + " " + k._2 + " " + k._3 + " " + missedConnection + " " + connection
		})
    
    	// Write the output to a file
    	result.saveAsTextFile("out")
    
    	// Shut down Spark
    	sc.stop()
    	val endTime: Long = System.currentTimeMillis
    	val timeTaken = endTime - startTime
    	print (timeTaken)
	}

  	/**
   	 * 
   	 */
   def convertToFlight (row : Array[String]) : ((String, String, String), (String, String, String), (Long, Long), (Long, Long)) = {
   		val carrierCode = row(8)
   		val year = row(0)
   		val timeZone = getTimeZone(row)
   		val depDelay = (row(31)).toFloat.toInt

		// CRS Time
		val CRSArrTime = timeToMinute(row(40))
		val CRSDepTime = timeToMinute(row(29))
		
		// Actual Time
		val arrTime = timeToMinute(row(41))
		val depTime = timeToMinute(row(30))

		// Get Elapsed Time
		val CRSElapsedTime = (row(50)).toFloat.toInt
		val actualElapsedTime = (row(51)).toFloat.toInt

		// Get Flight date
		val CRSDepFlightDate = getEpochMinutes(row(5))
		val depFlightDate = CRSDepFlightDate + depDelay

		// Airport Id
		val destAirportId = row(20)
		val originAirportId = row(11)

		// Get Time in epoch
		val CRSDepEpochTime = getDepartureEpochMinutes(CRSDepFlightDate, CRSDepTime)
		val depEpochTime = getDepartureEpochMinutes(depFlightDate, depTime)
		val CRSArrEpochTime = getArrivalEpochMinutes(CRSDepFlightDate, CRSElapsedTime, CRSDepTime, timeZone)
		val arrEpochTime = getArrivalEpochMinutes(depFlightDate, actualElapsedTime, depTime, timeZone)

		((carrierCode, year, destAirportId), (carrierCode, year, originAirportId), (CRSArrEpochTime, arrEpochTime), (CRSDepEpochTime, depEpochTime))
	}

  	/**
	 * Check whether the given flight is cancelled or not
	 * 
	 * @param row Record of flight OTP data
	 * @return true iff it cancelled. False, otherwise
	 */
	 def isCancelled (row : Array[String]) : Boolean = {
	 	val cancelled = (row(47)).toFloat.toInt
	 	cancelled == 1
	 }

  	/**
	 * Convert the given date in YYYY-MM-DD format into 
	 * the minutes since epoch
	 * 
	 * @param aDate Date
	 * @return Minutes since epoch
	 */
	 def getEpochMinutes (aDate : String) : Long = {
	 	val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
	 	var date : Date = null;
	 	try {
	 		date = simpleDateFormat.parse(aDate)
	 	} catch {
	 		case exception : ParseException => {
	 			exception.printStackTrace()
	 		}
	 	}
	 	date.getTime() / (60 * 1000).toLong
	 }

  	/**
	 * Return the timezone in minutes
	 * 
	 * @param row Record of flight OTP data
	 * @return Timezone
	 */
	 def getTimeZone (row : Array[String]) : Int = {
    	// hh:mm format
    	val CRSArrTime = timeToMinute(row(40))
    	val CRSDepTime = timeToMinute(row(29))

		// minutes format
		val CRSElapsedTime = (row(50)).toFloat.toInt
		CRSArrTime - CRSDepTime - CRSElapsedTime
	}

    /**
	 * Return the arrival date and time in epoch minutes
	 * 
	 * @param depFlight Departure flight date in epoch minutes 
	 * @param elapsedTime Duration of flight in minutes
	 * @param depTime Departure flight time in epoch minutes
	 * @param timezone Timezone in minutes
	 * @return Arrival Time
	 */
	 def getArrivalEpochMinutes (depFlight : Long, elapsedTime : Int, depTime : Int, timeZone : Int) : Long = {
	 		depFlight + elapsedTime + depTime + timeZone
	 }

    /**
	 * Return the departure date and time in epoch minutes
	 * 
	 * @param depFlight Departure flight date in epoch minutes
	 * @param depTime Departure flight time in epoch minutes
	 * @return Departure Time
	 */
	 def getDepartureEpochMinutes (depFlight : Long, depTime : Int) : Long = {
	 	depFlight + depTime
	 }

    /**
	 * Check whether the flights are a missed connection or not
	 * 
	 * @param CRSArrTime The scheduled arrival time
	 * @param CRSDepTime The scheduled departure time
	 * @return true iff they are missed connections. False, otherwise
	 */
	 def isMissedConnection (arrEpochTime : Long, depEpochTime : Long) : Boolean = {
	 	if (depEpochTime < arrEpochTime + 30) {
	 		return true
	 	}
	 	false
	 }

  	/**
	 * Check whether the flights are a connection or not
	 * 
	 * @param CRSArrTime The scheduled arrival time
	 * @param CRSDepTime The scheduled departure time
	 * @return true iff they are connections. False, otherwise
	 */
	 def isConnection (CRSArrEpochTime : Long, CRSDepEpochTime : Long) : Boolean = {
		if (CRSDepEpochTime >= CRSArrEpochTime + 30 && CRSDepEpochTime <= CRSArrEpochTime + 360) {
	 		return true
	 	}
	 	false
	 }

  	/**
	 * Convert time in hhmm format to minutes since day started
	 * 
	 * @param time The time in hhmm format
	 * @return Minutes minutes since day started
	 * @throws NumberFormatException
	 */
	 @throws(classOf[NumberFormatException])
	 def timeToMinute (time : String) : Int = {
	 	if (time == null || time.length() == 0) {
	 		throw new NumberFormatException()
	 	}

	 	val hhmm = (time).toFloat.toInt
	 	var hour = 0
	 	var minute = 0

	 	if (hhmm < 100) {
	 		minute = hhmm
	 	} else {
	 		hour = hhmm / 100
	 		minute = hhmm % 100
	 	}

	 	hour * 60 + minute
	 }

	/**
	 * Check whether the given record passes the validity test
	 * 
	 * @param row Record of flight OTP data
	 * @return true iff it passes. False, otherwise
	 */
	 def isValidRecord (row : Array[String]) : Boolean = {
	 	if (row.length != 110) {
	 		return false
	 	}
	 	true
	 }

	/**
	 * Check whether the given record passes the sanity test
	 * 
	 * @param row Record of flight OTP data
	 * @return true iff it passes sanity test. False, otherwise
	 */
	 def sanityTest (row : Array[String]) : Boolean = {

	 	try {

		// hh:mm format
		var CRSArrTime = timeToMinute(row(40))
		var CRSDepTime = timeToMinute(row(29))

		// Check for zero value
		if (CRSArrTime == 0 || CRSDepTime == 0) {
			return false
		}

		// minutes format
		var CRSElapsedTime = (row(50)).toFloat.toInt
		var timeZone = CRSArrTime - CRSDepTime - CRSElapsedTime

		// Check for modulo zero
		if (timeZone % 60 != 0) {
			return false;
		}

		var originAirportId = (row(11)).toFloat.toInt
		var destAirportId = (row(20)).toFloat.toInt
		var originAirportSeqId = (row(12)).toFloat.toInt
		var destAirportSeqId = (row(21)).toFloat.toInt
		var originCityMarketId = (row(13)).toFloat.toInt
		var destCityMarketId = (row(22)).toFloat.toInt
		var originStateFips = (row(17)).toFloat.toInt
		var destStateFips = (row(26)).toFloat.toInt
		var originWac = (row(19)).toFloat.toInt
		var destWac = (row(28)).toFloat.toInt
			
		// Check for Ids greater than zero
		if (originAirportId <= 0 || destAirportId <= 0 || originAirportSeqId <= 0 || 
			destAirportSeqId <= 0 || originCityMarketId <= 0 || destCityMarketId <= 0 || 
			originStateFips <= 0 || destStateFips <= 0 || originWac <= 0 || destWac <= 0) {
			return false
		}
			
		// Check for non-empty condition
		if (row(14).isEmpty() || row(23).isEmpty() || row(15).isEmpty() || row(24).isEmpty() ||
			row(16).isEmpty() || row(25).isEmpty() || row(18).isEmpty() || row(27).isEmpty()) {
			return false
		}

		// For flights that are not cancelled
		var cancelled = (row(47)).toFloat.toInt
		if (cancelled != 1) {
			var arrTime = timeToMinute(row(41))
			var depTime = timeToMinute(row(30))
			var actualElapsedTime = (row(51)).toFloat
			
			// Check for zero value
			var time = arrTime - depTime - actualElapsedTime - timeZone
			if (time != 0 && time % 1440 != 0) {
				return false
			}
				
			var arrDelay = (row(42)).toFloat
			var arrDelayMinutes = (row(43)).toFloat
			if (arrDelay > 0 && arrDelay != arrDelayMinutes) {
				return false
			} else if (arrDelay < 0 && arrDelayMinutes != 0) {
				return false
			}

			if (arrDelayMinutes >= 15 && ((row(44)).toFloat) != 1) {
				return false
			}
		}
				
		} catch {
			case exception : NumberFormatException => {
				return false
			}
		}

		true
	 }

  	/**
	 * Parse a CSV record given as string
	 * - Remove any quotes from record
	 * - Splits on each new column
	 * 
	 * @param record The record to parse 
	 * @return Array containing those records, null if couldn't parse
	 */
	 def parse (record : String) : Array[String] = {
	 	val line = record.replaceAll("\"", "").replaceAll(", ", ";")
	 	line.split(",")
	 }
}