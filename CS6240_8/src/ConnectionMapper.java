import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ConnectionMapper extends Mapper<Object, Text, Text, FlightDetail> {

	@Override
	public void map(Object key, Text value, Context context) 
	 throws IOException, InterruptedException {
		
	}
}