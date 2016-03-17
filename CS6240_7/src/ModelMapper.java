import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Mapper class for cleaning and synthesizing input
 * 
 * @author Adib Alwani
 */
public class ModelMapper extends Mapper<Object, Text, Text, FlightDetail> {
	
	@Override
	public void map(Object key, Text value, Context context) 
		throws IOException, InterruptedException {
		
		FlightHandler handler = new FlightHandler();
		String[] row = handler.parse(value.toString(), 110);
		if (row != null && handler.sanityTest(row, FlightHandler.TRAIN) && !handler.isCancelled(row)) {
			FlightDetail flightDetail = handler.getFlightDetails(row);
			String month = row[2];
			
			context.write(new Text(month), flightDetail);
		}
	}
}