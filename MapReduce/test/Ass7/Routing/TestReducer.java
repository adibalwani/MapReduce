import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.mapreduce.Reducer;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

/**
 * Reducer class to find possible connections 
 * 
 * @author Adib Alwani
 */
public class TestReducer extends Reducer<Text, Text, Text, Text> {
	
	/**
	 * Read files from the given folder and build a map:
	 * Key --> Origin,Destination,DayOfMonth
	 * Value --> ArrivalFlightNumber,DepartureFlightNumber,TotalDuration 
	 * 
	 * @param folderName Name of folder to read from
	 * @return Map instance as described
	 * @throws IOException
	 */
	private Map<String, String> getConnectionMap(String folderName) throws IOException {
		Map<String, String> map = new HashMap<String, String>();
		FileSystem fs = FileSystem.get(new Configuration());
        FileStatus[] status = fs.listStatus(new Path(folderName));
        
        // Read file and put in hash
        for (int i = 0; i < status.length; i++) {
        	BufferedReader br = new BufferedReader(new InputStreamReader(
        			fs.open(status[i].getPath())));
        	String line = br.readLine();
        	while (line != null) {
        		String[] vals = line.split(":");
        		map.put(vals[0], vals[1]);
        		line = br.readLine();
        	}
        }
        
        return map;
	}
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {
		
		String[] keys = key.toString().split("_");
		String year = keys[0];
		String month = keys[1];
		Map<String, String> map = getConnectionMap(key.toString());
	    
	    for (Text text : values) {
	    	String val = map.get(text.toString());
	    	if (val != null) {
	    		StringBuilder builder = new StringBuilder();
	    		builder.append(year);
	    		builder.append(',');
	    		builder.append(month);
	    		builder.append(',');
	    		builder.append(text.toString());
	    		builder.append(',');
	    		builder.append(val);
	    		context.write(new Text(builder.toString()), new Text(""));
	    	}
	    }
	}
}
