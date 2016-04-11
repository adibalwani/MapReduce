package edu.neu.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IntWritable implements Writable,Comparable<IntWritable> {
	private int value;
	
	public IntWritable() {
		super();
		// TODO Auto-generated constructor stub
	}

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
		// TODO Auto-generated method stub
		out.writeInt(value);
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
	value=in.readInt();	
	}

	public int compareTo(IntWritable o) {
		// TODO Auto-generated method stub
		return Integer.compare(this.value, o.value);
	}
	

}
