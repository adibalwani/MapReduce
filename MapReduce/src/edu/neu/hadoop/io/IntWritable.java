package edu.neu.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A serializable int object which implements a simple, efficient, serialization
 * protocol, based on {@link DataInput} and {@link DataOutput}
 * 
 * @author Rushikesh Badami
 * @modified Adib Alwani
 */
public class IntWritable implements Writable, Cloneable, Comparable<IntWritable> {

	private int value;
	
	public IntWritable() { }

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
		return Integer.compare(this.value, o.get());
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Writable) {
			IntWritable o = (IntWritable) obj;
			return o.value == this.value;
		}
		return super.equals(obj);
	}
	
	@Override
	public String toString() {
		return String.valueOf(value);
	}
	
	@Override
	public Object clone() throws CloneNotSupportedException {
		return new IntWritable(value);
	}

	public int get() {
		return value;
	}

	public void set(int value) {
		this.value = value;
	}
}
