import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import edu.neu.hadoop.conf.Configuration;
import edu.neu.hadoop.fs.FSDataOutputStream;
import edu.neu.hadoop.fs.FileSystem;
import edu.neu.hadoop.fs.Path;
import edu.neu.hadoop.io.BytesWritable;
import edu.neu.hadoop.io.Text;
import edu.neu.hadoop.mapreduce.Reducer;
import edu.neu.hadoop.mapreduce.Context;

import weka.classifiers.Classifier;
import weka.classifiers.bayes.NaiveBayes;
import weka.core.Attribute;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.DenseInstance;

/**
 * Reducer class for building Model
 * 
 * @author Adib Alwani
 */
public class ModelReducer extends Reducer<Text, FlightDetail, Text, BytesWritable> {
	
	/**
	 * Initialize an empty training set
	 * 
	 * @return Empty training set
	 */
	private Instances initTrainingSet() {
		// Declare numeric attributes
		Attribute dayOfMonth = new Attribute("dayOfMonth");
		Attribute dayOfWeek = new Attribute("dayOfWeek");
		Attribute originAirportId = new Attribute("originAirportId");
		Attribute destAirportId = new Attribute("destAirportId");
		Attribute CRSDepTime = new Attribute("CRSDepTime");
		Attribute CRSArrTime = new Attribute("CRSArrTime");
		
		// Declare nominal attribute along with its values
		List<String> holidayAttributes = new ArrayList<String>();
		holidayAttributes.add("true");
		holidayAttributes.add("false");
		Attribute holiday = new Attribute("holiday", holidayAttributes);
		
		// Declare numeric attribute with a string converted to its hash value
		Attribute carrierCode = new Attribute("carrierCode");
		
		// Declare the class attribute along with its values
		List<String> classAttributes = new ArrayList<String>();
		classAttributes.add("true");
		classAttributes.add("false");
		Attribute delay = new Attribute("delay", classAttributes);
		
		// Declare the feature vector
		ArrayList<Attribute> wekaAttributes = new ArrayList<Attribute>();
		wekaAttributes.add(dayOfMonth);
		wekaAttributes.add(dayOfWeek);
		wekaAttributes.add(carrierCode);
		wekaAttributes.add(originAirportId);
		wekaAttributes.add(destAirportId);
		wekaAttributes.add(CRSDepTime);
		wekaAttributes.add(CRSArrTime);
		wekaAttributes.add(delay);
		wekaAttributes.add(holiday);
		
		// Create an empty training set
		Instances trainingSet = new Instances("Model", wekaAttributes, 0);
		// Set class index
		trainingSet.setClassIndex(7);
		
		return trainingSet;
	}
	
	/**
	 * Add the given flight details instance to the Training Set
	 * 
	 * @param flightDetail Details of a flight 
	 * @param trainingSet Training set instance
	 */
	private void addInstance(FlightDetail flightDetail, Instances trainingSet) {
		Instance instance = new DenseInstance(9);
		instance.setValue(trainingSet.attribute(0), flightDetail.getDayOfMonth().get());
		instance.setValue(trainingSet.attribute(1), flightDetail.getDayOfWeek().get());
		instance.setValue(trainingSet.attribute(2), 
				flightDetail.getCarrierCode().toString().hashCode());
		instance.setValue(trainingSet.attribute(3), flightDetail.getOriginAirportId().get());
		instance.setValue(trainingSet.attribute(4), flightDetail.getDestAirportId().get());
		instance.setValue(trainingSet.attribute(5), flightDetail.getCRSDepTime().get());
		instance.setValue(trainingSet.attribute(6), flightDetail.getCRSArrTime().get());
		instance.setValue(trainingSet.attribute(7), flightDetail.getDelay().get() + "");
		instance.setValue(trainingSet.attribute(8), flightDetail.getHoliday().get() + "");
		
		trainingSet.add(instance);
	}
	
	/**
	 * Create a naïve bayes classifier based on the training set
	 * 
	 * @param trainingSet Training set instance
	 * @return Naïve bayes classifier
	 * @throws Exception
	 */
	private Classifier classify(Instances trainingSet) throws Exception {
		NaiveBayes naiveBayes = new NaiveBayes();
		naiveBayes.setUseKernelEstimator(true);
		naiveBayes.buildClassifier(trainingSet);
		return naiveBayes;
	}
	
	@Override
	public void reduce(Text key, Iterable<FlightDetail> values, Context context)  
			throws IOException, InterruptedException {
		
		Instances trainingSet = initTrainingSet();
		
		for (FlightDetail flightDetail : values) {
			addInstance(flightDetail, trainingSet);
		}
		
		try {
			Classifier model = classify(trainingSet);
			FileSystem fileSystem = FileSystem.get(URI.create("models/"),
					new Configuration());
			FSDataOutputStream fsDataOutputStream = fileSystem.create(
					new Path("models/" + key.toString()));
			weka.core.SerializationHelper.write(fsDataOutputStream, model);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
