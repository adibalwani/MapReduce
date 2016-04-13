package edu.neu.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A serializable int object which implements a simple, efficient, serialization
 * protocol, based on {@link DataInput} and {@link DataOutput}
 * 
 * @author Rushikesh Badami
 */
public class IntWritable implements Writable, Comparable<IntWritable> {

	private int value;

	public IntWritable(int value) {
		this.value = value;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(value);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		value = in.readInt();
	}

	@Override
	public int compareTo(IntWritable o) {
		return Integer.compare(this.value, o.getValue());
	}

	public int getValue() {
		return value;
	}
}
