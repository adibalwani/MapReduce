import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper class to emit request data
 * 
 * @author Adib Alwani
 */
public class TestMapper extends Mapper<Object, Text, Text, Text> {
	
	@Override
	public void map(Object key, Text value, Context context) 
	 throws IOException, InterruptedException {
		
		FlightHandler flightHandler = new FlightHandler();
		String[] row = flightHandler.parse(value.toString(), 6);
		
		if (row != null) {
			String year = row[0];
			String month = row[1];
			String dayOfMonth = row[2];
			String origin = row[3];
			String destination = row[4];
			
			context.write(new Text(month + "_" + year), new Text(origin + "," + destination + "," + dayOfMonth));
		}
	}
}
