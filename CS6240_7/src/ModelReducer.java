import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ModelReducer extends Reducer<Text, FlightDetail, Text, Text> {
	
	public void reduce(Text key, Iterable<FlightDetail> values, Context context)  
			throws IOException, InterruptedException {
		for (FlightDetail flightDetail : values) {
			context.write(key, new Text(flightDetail.toString()));
		}
	}

}
