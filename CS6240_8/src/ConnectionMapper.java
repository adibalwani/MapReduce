import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper class to emit flight Details
 * 
 * @author Adib Alwani, Rachit Puri
 */
public class ConnectionMapper extends Mapper<Object, Text, Text, FlightDetail> {
	
	@Override
	public void map(Object key, Text value, Context context) 
	 throws IOException, InterruptedException {
		
		FlightHandler flightHandler = new FlightHandler();
		String[] row = flightHandler.parse(value.toString(), 111);
		
		if (row != null && flightHandler.sanityTest(row, FlightHandler.TEST)) {
			String carrierCode = row[8];
			String year = row[0];
			String month = row[2];
			int timeZone = flightHandler.getTimeZone(row);
			
			// CRS Time
			int CRSDepTime = flightHandler.timeToMinute(row[29]);
			
			// Get Elapsed Time
			int CRSElapsedTime = (int) Float.parseFloat(row[50]);
			
			// Get Flight date
			long CRSDepFlightDate = flightHandler.getEpochMinutes(row[5]);
			
			// Airport Id
			String destAirportId = row[20];
			String originAirportId = row[11];
			
			// Get Scheduled Time
			long CRSDepEpochTime = flightHandler.getDepartureEpochMinutes(
					CRSDepFlightDate, CRSDepTime);
			long CRSArrEpochTime = flightHandler.getArrivalEpochMinutes(CRSDepFlightDate,
					CRSElapsedTime, CRSDepTime, timeZone);
			
			// Arrival Flight
			FlightDetail flightDetailArrival = flightHandler.getFlightDetails(row);
			flightDetailArrival.setCRSArrTimeEpoch(new LongWritable(CRSArrEpochTime));
			flightDetailArrival.setCRSDepTimeEpoch(new LongWritable(CRSDepEpochTime));
			flightDetailArrival.setArrival(new BooleanWritable(true));
			
			// Departure Flight
			FlightDetail flightDetailDeparture = flightHandler.getFlightDetails(row);
			flightDetailDeparture.setCRSArrTimeEpoch(new LongWritable(CRSArrEpochTime));
			flightDetailDeparture.setCRSDepTimeEpoch(new LongWritable(CRSDepEpochTime));
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
