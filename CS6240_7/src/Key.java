import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


/**
 * Class representing Key and whether it is a model or not
 * 
 * @author Adib Alwani
 */
public class Key implements WritableComparable<Key> {
	
	private Text key;
	private BooleanWritable model;
	
	public Key() {
		key = new Text();
		model = new BooleanWritable();
	}
	
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		key.readFields(dataInput);
		model.readFields(dataInput);
	}
	
	@Override
	public void write(DataOutput dataOutput) throws IOException {
		key.write(dataOutput);
		model.write(dataOutput);
	}
	
	@Override
	public int compareTo(Key o) {
		if (model.get()) {
			return -1;
		}
		
		return key.compareTo(o.getKey());
	}

	public Text getKey() {
		return key;
	}

	public void setKey(Text key) {
		this.key = key;
	}

	public BooleanWritable getModel() {
		return model;
	}

	public void setModel(BooleanWritable model) {
		this.model = model;
	}
}
