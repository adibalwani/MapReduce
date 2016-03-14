import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import weka.classifiers.Classifier;
import weka.core.SerializationHelper;

/**
 * Reducer class for testing Model
 * 
 * @author Adib Alwani
 */
public class TestReducer extends Reducer<Key, FlightDetailModelPair, Text, Text> {
	
	/**
	 * 
	 * 
	 * @param pair
	 * @return
	 * @throws Exception
	 */
	private Classifier getClassifier(FlightDetailModelPair pair) throws Exception {
		String model = pair.getModel().toString();
		InputStream inputStream = new ByteArrayInputStream(model.getBytes());
		return (Classifier) SerializationHelper.read(inputStream);
	}
	
	@Override
	public void reduce(Key key, Iterable<FlightDetailModelPair> values, Context context)  
			throws IOException, InterruptedException {
		
		Iterator<FlightDetailModelPair> iterator = values.iterator();
		Classifier classifier;
		
		try {
			classifier = getClassifier(iterator.next());
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		int count = 0;
		
		while (iterator.hasNext()) {
			count++;
		}
		
		context.write(new Text(count + ""), new Text(""));
	}
}
