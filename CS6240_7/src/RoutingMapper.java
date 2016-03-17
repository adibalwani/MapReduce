import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class RoutingMapper extends Mapper<Object, Text, Text, RoutingFlight>{

	@Override
	public void map(Object key, Text value, Context context) 
	 throws IOException, InterruptedException {
		RoutingHandler routing = new RoutingHandler();
		String[] row = routing.parse(value.toString(), 110);
		if (row != null && routing.sanityTest(row) && !routing.isCancelled(row)) {
			RoutingFlight routingFlight = routing.getFlightDetails(row);
			String year = row[0];
			String month = row[2];
			
			context.write(new Text(month + "_" + year), flightDetail);
		}
	}
}