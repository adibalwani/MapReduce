package edu.neu.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A serializable float object which implements a simple, efficient,
 * serialization protocol, based on {@link DataInput} and {@link DataOutput}
 * 
 * @author Rushikesh Badami
 * @modified Adib Alwani
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
		return Float.compare(this.value, o.get());
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Writable) {
			FloatWritable o = (FloatWritable) obj;
			return o.value == this.value;
		}
		return super.equals(obj);
	}
	
	@Override
	public String toString() {
		return String.valueOf(value);
	}

	public float get() {
		return value;
	}

	public void set(float value) {
		this.value = value;
	}
}
