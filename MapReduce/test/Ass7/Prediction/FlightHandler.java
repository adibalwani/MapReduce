import edu.neu.hadoop.io.BooleanWritable;
import edu.neu.hadoop.io.IntWritable;
import edu.neu.hadoop.io.Text;
import edu.neu.hadoop.mapreduce.Context;


/**
 * Helper class for handling correct parsing of the Flight OTP data 
 * 
 * @author Adib Alwani
 */
public class FlightHandler {
	
	public static final int TRAIN = 1;
	public static final int TEST = 2; 
	
	/**
	 * Parse a CSV record given as string
	 * - Remove any quotes from record
	 * - Splits on each new column
	 * 
	 * @param record The record to parse 
	 * @return Array containing those records, null if couldn't parse
	 */
	public String[] parse(String record, int column) {
		
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
	 * Get the required flight details
	 * 
	 * @param row Record of flight OTP data
	 * @return Flight details
	 */
	public FlightDetail getFlightDetails(String[] row) {
		FlightDetail flightDetail = new FlightDetail();
		
		int dayOfMonth = (int) Float.parseFloat(row[3]);
		flightDetail.setDayOfMonth(new IntWritable(dayOfMonth));
		
		int dayOfWeek = (int) Float.parseFloat(row[4]);
		flightDetail.setDayOfWeek(new IntWritable(dayOfWeek));
		
		String carrierCode = row[8];
		flightDetail.setCarrierCode(new Text(carrierCode));
		
		int originAirportId = (int) Float.parseFloat(row[11]);
		flightDetail.setOriginAirportId(new IntWritable(originAirportId));
		
		int destAirportId = (int) Float.parseFloat(row[20]);
		flightDetail.setDestAirportId(new IntWritable(destAirportId));
		
		int CRSDepTime = (int) Float.parseFloat(row[29]);
		flightDetail.setCRSDepTime(new IntWritable(CRSDepTime));
		
		int CRSArrTime = (int) Float.parseFloat(row[40]);
		flightDetail.setCRSArrTime(new IntWritable(CRSArrTime));
		
		if (!row[43].equals("NA")) {
			int arrDelayMinutes = (int) Float.parseFloat(row[43]);
			flightDetail.setDelay(new BooleanWritable(arrDelayMinutes > 0));
		} else {
			flightDetail.setDelay(new BooleanWritable(true));
		}
		
		int month = (int) Float.parseFloat(row[2]);
		flightDetail.setHoliday(new BooleanWritable(isHoliday(month, dayOfMonth)));
		
		return flightDetail;
	}
	
	/**
	 * Checks whether the provided date is close to a holiday
	 * 
	 * @param month Month in the year
	 * @param dayOfMonth Day of the month
	 * @return true iff the date is close to a holiday. False, otherwise
	 */
	private boolean isHoliday(int month, int dayOfMonth) {
		// New Year
		if (month == 1 && dayOfMonth == 1) {
			return true;
		}
		
		// Thanksgiving
		if (month == 11 && dayOfMonth > 20 && dayOfMonth < 30) {
			return true;
		}
		
		// Christmas
		if (month == 12 && dayOfMonth > 20 && dayOfMonth <= 31) {
			return true;
		}
				
		return false;
	}
	
	/**
	 * Check whether the given record passes the sanitary test
	 * 
	 * @param row Record of flight OTP data
	 * @param type Type of input record
	 * @return true iff it passes sanity test. False, otherwise
	 */
	public boolean sanityTest(String[] row, int type) {
		try {
			
			// hh:mm format
			int CRSArrTime = timeToMinute(row[40]);
			int CRSDepTime = timeToMinute(row[29]);
			
			// Check for zero value
			if (CRSArrTime == 0 || CRSDepTime == 0) {
				return false;
			}
			
			// minutes format
			int CRSElapsedTime = (int) Float.parseFloat(row[50]);
			int timeZone = CRSArrTime - CRSDepTime - CRSElapsedTime;
			
			// Check for modulo zero
			if (timeZone % 60 != 0) {
				return false;
			}
			
			int originAirportId = (int) Float.parseFloat(row[11]);
			int destAirportId = (int) Float.parseFloat(row[20]);
			int originAirportSeqId = (int) Float.parseFloat(row[12]);
			int destAirportSeqId = (int) Float.parseFloat(row[21]);
			int originCityMarketId = (int) Float.parseFloat(row[13]);
			int destCityMarketId = (int) Float.parseFloat(row[22]);
			int originStateFips = (int) Float.parseFloat(row[17]);
			int destStateFips = (int) Float.parseFloat(row[26]);
			int originWac = (int) Float.parseFloat(row[19]);
			int destWac = (int) Float.parseFloat(row[28]);
			
			// Check for Ids greater than zero
			if (originAirportId <= 0 || destAirportId <= 0 || originAirportSeqId <= 0 || 
					destAirportSeqId <= 0 || originCityMarketId <= 0 || destCityMarketId <= 0 || 
					originStateFips <= 0 || destStateFips <= 0 || originWac <= 0 || destWac <= 0) {
				return false;
			}
			
			// Check for non-empty condition
			if (row[14].isEmpty() || row[23].isEmpty() || row[15].isEmpty() || row[24].isEmpty() ||
					row[16].isEmpty() || row[25].isEmpty() || row[18].isEmpty() || row[27].isEmpty()) {
				return false;
			}
			
			// Cancel further sanity test
			if (type == TEST) {
				return true;
			}
			
			// For flights that are not cancelled
			int cancelled = (int) Float.parseFloat(row[47]);
			if (cancelled != 1) {
				int arrTime = timeToMinute(row[41]);
				int depTime = timeToMinute(row[30]);
				int actualElapsedTime = (int) Float.parseFloat(row[51]);
				
				// Check for zero value
				int time = arrTime - depTime - actualElapsedTime - timeZone;
				if (time != 0 && time % 1440 != 0) {
					return false;
				}
				
				int arrDelay = (int) Float.parseFloat(row[42]);
				int arrDelayMinutes = (int) Float.parseFloat(row[43]);
				if (arrDelay > 0 && arrDelay != arrDelayMinutes) {
					return false;
				} else if (arrDelay < 0 && arrDelayMinutes != 0) {
					return false;
				}
				
				if (arrDelayMinutes >= 15 && ((int) Float.parseFloat(row[44])) != 1) {
					return false;
				}
			}
			
		} catch (NumberFormatException exception) {
			
			return false;
			
		}
		
		return true;
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
	 * Check whether the given flight is cancelled or not
	 * 
	 * @param row Record of flight OTP data
	 * @return true iff it cancelled. False, otherwise
	 */
	public boolean isCancelled(String[] row) {
		int cancelled = (int) Float.parseFloat(row[47]);
		return cancelled == 1;
	}
	
	/**
	 * Check whether the object is a model
	 * 
	 * @param record Flight OTP record
	 * @return true iff object is model. False, otherwise
	 */
	public boolean isModel(String record) {
		String[] modelPair = record.split("-->");
		return modelPair.length == 2;
	}

}
