package edu.neu.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A serializable boolean object which implements a simple, efficient,
 * serialization protocol, based on {@link DataInput} and {@link DataOutput}
 * 
 * @author Rushikesh Badami
 */
public class BooleanWritable implements Writable, Comparable<BooleanWritable> {

	private boolean value;
	
	public BooleanWritable() { }

	public BooleanWritable(boolean value) {
		this.value = value;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeBoolean(value);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		value = in.readBoolean();
	}

	@Override
	public int compareTo(BooleanWritable o) {
		return Boolean.compare(this.value, o.get());
	}

	public boolean get() {
		return value;
	}

	public void set(boolean value) {
		this.value = value;
	}
}
