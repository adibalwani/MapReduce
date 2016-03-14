import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Mapper class to test the built model
 * 
 * @author Adib Alwani
 */
public class TestMapper extends Mapper<Object, Text, Key, FlightDetailModelPair> {

	@Override
	public void map(Object inputKey, Text value, Context context) 
		throws IOException, InterruptedException {
		
		FlightHandler handler = new FlightHandler();
		FlightDetailModelPair pair = new FlightDetailModelPair();
		Key key = new Key();
		String record = value.toString();
		
		if (handler.isModel(record)) {
			String[] modelPair = record.split("::");
			String[] keys = modelPair[0].split("_");
			String flightNumber = keys[0];
			String month = keys[1];
			String year = keys[2];
			key.setKey(new Text(flightNumber + " " + month + " " + year));
			key.setModel(new BooleanWritable(true));
			pair.setModel(new Text(modelPair[1]));
		} else {
			String[] row = handler.parse(record, 112);
			if (row != null) {
				row = Arrays.copyOfRange(row, 1, row.length);
			}
			if (row != null && handler.sanityTest(row)) {
				FlightDetail flightDetail = handler.getFlightDetails(row);
				String year = row[0];
				String month = row[2];
				String flightNumber = row[10];
				key.setKey(new Text(flightNumber + " " + month + " " + year));
				key.setModel(new BooleanWritable(false));
				pair.setFlightDetail(flightDetail);
			} else {
				return;
			}
		}
		
		context.write(key, pair);
	}
}
