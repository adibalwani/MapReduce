import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ConnectionMapper extends Mapper<Object, Text, Text, FlightDetail> {

	/**
	 * Parse a CSV record given as string
	 * - Remove any quotes from record
	 * - Splits on each new column
	 * 
	 * @param record The record to parse 
	 * @return Array containing those records, null if couldn't parse
	 */
	private String[] parse(String record, int column) {
		
		String[] ans = new String[column];
		int col = 0;
		int len = record.length();
		StringBuilder builder = new StringBuilder();
		char[] arr = record.toCharArray();
		
		for (int i = 0; i < len; i++) {
			char ch = arr[i];
			
			// Remove quotes 
			if (ch == '\"') {
				continue;
			}
			
			if (ch != ',') {
				// No Split condition
				builder.append(ch);
				
				if (i == len - 1) {
					ans[col] = builder.toString();
				}
			} else if (i + 1 < len && arr[i + 1] == ' ') {
				// Ignore condition
				builder.append(ch);
			} else {
				// Split condition
				if (i == len - 1) {
					col = 0;
					break;
				}
				
				ans[col] = builder.toString();
				builder.setLength(0);
				col++;
			}
		}
		
		if (col < column - 1) {
			return null;
		}
		
		return ans;
	}
	
	/**
	 * Convert time in hhmm format to minutes since day started
	 * 
	 * @param time The time in hhmm format
	 * @return Minutes minutes since day started
	 * @throws NumberFormatException
	 */
	private int timeToMinute(String time) throws NumberFormatException {
		if (time == null || time.length() == 0) {
			throw new NumberFormatException();
		}
		
		int hhmm = (int) Float.parseFloat(time);
		int hour = 0;
		int minute = 0;
		
		if (hhmm < 100) {
			minute = hhmm;
		} else {
			hour = hhmm / 100;
			minute = hhmm % 100; 
		}
		
		return hour * 60 + minute;
	}
	
	/**
	 * Convert the given date in YYYY-MM-DD format into 
	 * the minutes since epoch
	 * 
	 * @param aDate Date
	 * @return Minutes since epoch
	 */
	private long getEpochMinutes(String aDate) {
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Date date = null;
		try {
			date = simpleDateFormat.parse(aDate);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return date.getTime() / (long) (60 * 1000);
	}
	
	/**
	 * Return the timezone in minutes
	 * 
	 * @param row Record of flight OTP data
	 * @return Timezone
	 */
	private int getTimeZone(String[] row) {
		// hh:mm format
		int CRSArrTime = timeToMinute(row[40]);
		int CRSDepTime = timeToMinute(row[29]);
		
		// minutes format
		int CRSElapsedTime = (int) Float.parseFloat(row[50]);
		return CRSArrTime - CRSDepTime - CRSElapsedTime;
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
	private long getArrivalEpochMinutes(long depFlight, int elapsedTime, int depTime, int timeZone) {
		return depFlight + elapsedTime + depTime + timeZone;
	}
	
	/**
	 * Return the departure date and time in epoch minutes
	 * 
	 * @param depFlight Departure flight date in epoch minutes
	 * @param depTime Departure flight time in epoch minutes
	 * @return Departure Time
	 */
	private long getDepartureEpochMinutes(long depFlight, int depTime) {
		return depFlight + depTime;
	}
	
	
	@Override
	public void map(Object key, Text value, Context context) 
	 throws IOException, InterruptedException {
		
		FlightHandler flightHandler = new FlightHandler();
		
		String[] row = parse(value.toString(), 110);
		if (row != null && flightHandler.sanityTest(row, FlightHandler.TEST)) {
			String carrierCode = row[8];
			String year = row[0];
			int timeZone = getTimeZone(row);
			// CRS Time
			int CRSDepTime = timeToMinute(row[29]);
			
			// Get Elapsed Time
			int CRSElapsedTime = (int) Float.parseFloat(row[50]);
			
			// Get Flight date
			long CRSDepFlightDate = getEpochMinutes(row[5]);
			
			// Airport Id
			String destAirportId = row[23];
			String originAirportId = row[14];
			
			String month = row[2];
			
			// Get Time in epoch
			long CRSDepEpochTime = getDepartureEpochMinutes(CRSDepFlightDate, CRSDepTime);
			//long depEpochTime = getDepartureEpochMinutes(depFlightDate, depTime);
			long CRSArrEpochTime = getArrivalEpochMinutes(CRSDepFlightDate, CRSElapsedTime, CRSDepTime, timeZone);
			//long arrEpochTime = getArrivalEpochMinutes(depFlightDate, actualElapsedTime, depTime, timeZone);
			
			FlightDetail flightDetailArrival = flightHandler.getFlightDetails(row);
			flightDetailArrival.setCrsArrTimeEpoch(new LongWritable(CRSArrEpochTime));
			flightDetailArrival.setCrsDepTimeEpoch(new LongWritable(CRSDepEpochTime));
			flightDetailArrival.setArrival(new BooleanWritable(true));
			
			FlightDetail flightDetailDeparture = flightHandler.getFlightDetails(row);
			flightDetailDeparture.setCrsArrTimeEpoch(new LongWritable(CRSArrEpochTime));
			flightDetailDeparture.setCrsDepTimeEpoch(new LongWritable(CRSDepEpochTime));
			flightDetailDeparture.setArrival(new BooleanWritable(false));
			
			// Emit Arrival
			context.write(new Text(carrierCode + " " + month + " " + year + " " + destAirportId), 
					flightDetailArrival);
			
			// Emit Departure
			context.write(new Text(carrierCode + " " + month + " " + year + " " + originAirportId), 
					flightDetailDeparture);
		}
	}
}