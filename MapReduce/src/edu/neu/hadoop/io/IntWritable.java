package edu.neu.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IntWritable implements Writable,Comparable<IntWritable> {
	private int value;
	
	
	public IntWritable(int value) {
		super();
		this.value = value;
	}

	public int getValue() {
		return value;
	}

	public void setValue(int value) {
		this.value = value;
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(value);
	}
	
	public void readFields(DataInput in) throws IOException {
		value=in.readInt();	
	}

	public int compareTo(IntWritable o) {
		return Integer.compare(o.value,this.value);
	}
	

}
