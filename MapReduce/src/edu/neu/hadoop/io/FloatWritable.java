package edu.neu.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FloatWritable  implements Writable,Comparable<FloatWritable>{
	private float value;
	
	public FloatWritable() {
		super();
		// TODO Auto-generated constructor stub
	}

	public FloatWritable(float value) {
		super();
		this.value = value;
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeFloat(value);
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		value=in.readFloat();
	}

	public int compareTo(FloatWritable o) {
		// TODO Auto-generated method stub
		return Float.compare(this.value, o.value);
	}

}