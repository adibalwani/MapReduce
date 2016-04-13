package edu.neu.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A serializable float object which implements a simple, efficient,
 * serialization protocol, based on {@link DataInput} and {@link DataOutput}
 * 
 * @author Rushikesh Badami
 */
public class FloatWritable implements Writable, Comparable<FloatWritable> {

	private float value;

	public FloatWritable() { }
	
	public FloatWritable(float value) {
		this.value = value;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeFloat(value);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		value = in.readFloat();
	}

	@Override
	public int compareTo(FloatWritable o) {
		return Float.compare(this.value, o.getValue());
	}

	public float getValue() {
		return value;
	}

}
