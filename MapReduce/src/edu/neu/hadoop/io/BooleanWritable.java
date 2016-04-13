package edu.neu.hadoop.io;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;

public class BooleanWritable implements Writable,Comparable<BooleanWritable>{
	private boolean value;
	
	
	public BooleanWritable(boolean value) {
		super();
		this.value = value;
	}

	public void write(DataOutput out) throws IOException {
	out.writeBoolean(value);	
	}

	public void readFields(DataInput in) throws IOException {
	value=in.readBoolean();
	}


	public int compareTo( BooleanWritable o) {
	return Boolean.compare(o.value,this.value);
	}
	
}
