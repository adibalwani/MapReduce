import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instances;

/**
 * Reducer class for building Model
 * 
 * @author Adib Alwani
 */
public class ModelReducer extends Reducer<Text, FlightDetail, Text, Text> {
	
	private Instances initTraingSet() {
		Instances trainingSe
	}
	
	public void reduce(Text key, Iterable<FlightDetail> values, Context context)  
			throws IOException, InterruptedException {
		
		for (FlightDetail flightDetail : values) {
			int monthInt = flightDetail.getMonth().get();
			Attribute month = new Attribute(String.valueOf(monthInt));
			
		}
		
		context.write(key, new Text(""));
	}
}
