import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;

import org.apache.hadoop.mapreduce.Reducer;
import java.util.List;
import java.util.ArrayList;
import java.io.BufferedWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import weka.classifiers.Classifier;
import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.SerializationHelper;

/**
 * Reducer class to find possible connections 
 * 
 * @author Adib Alwani, Rachit Puri
 */
public class ConnectionReducer extends Reducer<Text, FlightDetail, Text, Text> {

	/**
	 * Check whether the flights are a connection or not
	 * 
	 * @param CRSArrTime The scheduled arrival time
	 * @param CRSDepTime The scheduled departure time
	 * @return true iff they are connections. False, otherwise
	 */
	private boolean isConnection(long CRSArrEpochTime, long CRSDepEpochTime) {
		if (CRSDepEpochTime >= CRSArrEpochTime + 30 && CRSDepEpochTime <= CRSArrEpochTime + 60) {
			return true;
		}
		return false;
	}
	
	/**
	 * Get the trained classifier
	 * 
	 * @param fileName The name of the serialized classifier
	 * @return Trained classifier 
	 * @throws Exception
	 */
	private Classifier getClassifier(String fileName) throws Exception {
		Path path = new Path("models/" + fileName);
	    FileSystem fileSystem = path.getFileSystem(new Configuration());
	    FSDataInputStream inputStream = fileSystem.open(path);
		return (Classifier) SerializationHelper.read(inputStream);
	}
	
	/**
	 * Get the dense instance from the given flight details
	 * 
	 * @param flightDetail Details of a flight
	 * @param set Instance set
	 * @return Instance of flight details
	 */
	private Instance getInstance(FlightDetail flightDetail, Instances set) {
		Instance instance = new DenseInstance(9);
		instance.setValue(set.attribute(0), flightDetail.getDayOfMonth().get());
		instance.setValue(set.attribute(1), flightDetail.getDayOfWeek().get());
		instance.setValue(set.attribute(2), 
				flightDetail.getCarrierCode().toString().hashCode());
		instance.setValue(set.attribute(3), flightDetail.getOriginAirportId().get());
		instance.setValue(set.attribute(4), flightDetail.getDestAirportId().get());
		instance.setValue(set.attribute(5), flightDetail.getCRSDepTime().get());
		instance.setValue(set.attribute(6), flightDetail.getCRSArrTime().get());
		instance.setValue(set.attribute(8), flightDetail.getHoliday().get() + "");
		
		return instance;
	}
	
	/**
	 * Initialize an empty set
	 * 
	 * @return Empty set
	 */
	private Instances initSet() {
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
		
		// Create an empty set
		Instances set = new Instances("Model", wekaAttributes, 0);
		// Set class index
		set.setClassIndex(7);
		
		return set;
	}
	
	/**
	 * Get the duration of the flight
	 * 
	 * @param CRSArrTime The scheduled arrival time
	 * @param CRSDepTime The scheduled departure time
	 * @return Duration in minutes
	 */
	private long getFlightDuration(long CRSArrTime, long CRSDepTime) {
		return CRSArrTime - CRSDepTime;
	}
	
	/**
	 * Predict whether the flight will be delayed or not
	 * 
	 * @param classifier Classifier to classify instance
	 * @param set Instance Set
	 * @param flightDetail Instance of flight details
	 * @return true iff flight is delayed. False, otherwise
	 */
	private boolean isDelayed(Classifier classifier, Instances set, FlightDetail flightDetail) {
		Instance arrInstance = getInstance(flightDetail, set);
		arrInstance.setDataset(set);
		try {
			double[] fDistribution = classifier.distributionForInstance(arrInstance);
			if (fDistribution[0] < fDistribution[1]) {
				return true;
			}
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		
		return false;
	}
	
	@Override
	public void reduce(Text key, Iterable<FlightDetail> values, Context context) 
			throws IOException, InterruptedException {
		
		String[] keys = key.toString().split(" ");
		String carrierCode = keys[0];
		String month = keys[1];
		String year = keys[2];
		String airportId = keys[3];
		String folderName = month + "_" + year;
		
		List<FlightDetail> arrList = new ArrayList<FlightDetail>();
		List<FlightDetail> depList = new ArrayList<FlightDetail>();
		Classifier classifier;
		Instances set = initSet();
		
		try {
			classifier = getClassifier(month);
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
		
		// Create folder with carrier code as file name
		FileSystem fileSystem = FileSystem.get(URI.create(folderName),
				new Configuration());
		BufferedWriter writer = new BufferedWriter(
				new OutputStreamWriter(
						fileSystem.create(new Path(folderName + "/" + carrierCode + "_" + airportId))
				)
		);
		
		// Add in list
		for (FlightDetail flight : values) {
			FlightDetail flightDetail = new FlightDetail().copy(flight);
			
			if (flight.isArrival().get()) {
				arrList.add(flightDetail);
			} else {
				depList.add(flightDetail);
			}
		}
		
		// Find Connections
		for (FlightDetail arr : arrList) {
			String origin = arr.getOrigin().toString();
			int arrFlightNumber = arr.getFlightNumber().get();
			long hop1ArrEpoch = arr.getCRSArrTimeEpoch().get();
			long hop1DepEpoch = arr.getCRSDepTimeEpoch().get();
			long hop1Duration = getFlightDuration(hop1ArrEpoch, hop1DepEpoch);
			boolean arrDelay = isDelayed(classifier, set, arr);
			
			for (FlightDetail dep : depList) {
				long hop2DepEpoch = dep.getCRSDepTimeEpoch().get();
				
				if (isConnection(hop1ArrEpoch, hop2DepEpoch)) {
					if (arrDelay) {
						StringBuilder builder = new StringBuilder();
						builder.append(year);
						builder.append(',');
						builder.append(month);
						builder.append(',');
						builder.append(arr.getDayOfMonth().get());
						builder.append(',');
						builder.append(arr.getOrigin().toString());
						builder.append(',');
						builder.append(dep.getDestination().toString());
						builder.append(',');
						builder.append(arr.getFlightNumber().get());
						builder.append(',');
						builder.append(dep.getFlightNumber().get());
						
						// Missed Connection
						context.write(new Text(builder.toString()), new Text(""));
						continue;
					}
					
					String dest = dep.getDestination().toString();
					int destFlightNumber = dep.getFlightNumber().get();
					long hop2ArrEpoch = dep.getCRSArrTimeEpoch().get();
					long hop2Duration = getFlightDuration(hop2ArrEpoch, hop2DepEpoch);
					long waitingTime = getFlightDuration(hop2DepEpoch, hop1ArrEpoch);
					long totalDuration = hop1Duration + hop2Duration + waitingTime;
					writer.write(origin + "," + dest + "," + arr.getDayOfMonth().get() + ":" 
							+ arrFlightNumber + "," + destFlightNumber + "," + totalDuration);
					writer.newLine();
				}
			}
		}
		
		writer.close();
	}
}
