import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Class representing flight details and model
 * 
 * @author Adib Alwani, Rachit Puri
 */
public class FlightDetailModelPair implements Writable {
	
	private Text model;
	private FlightDetail flightDetail;
	
	public FlightDetailModelPair() {
		model = new Text();
		flightDetail = new FlightDetail();
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		model.readFields(dataInput);
		flightDetail.readFields(dataInput);
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		model.write(dataOutput);
		flightDetail.write(dataOutput);
	}
	
	/**
	 * Check whether the object is a model
	 * 
	 * @return true iff object is model. False, otherwise
	 */
	public boolean isModel() {
		return model.getLength() == 0;
	}

	public Text getModel() {
		return model;
	}

	public void setModel(Text model) {
		this.model = model;
	}

	public FlightDetail getFlightDetail() {
		return flightDetail;
	}

	public void setFlightDetail(FlightDetail flightDetail) {
		this.flightDetail = flightDetail;
	}
}
