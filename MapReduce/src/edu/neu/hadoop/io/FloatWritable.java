package edu.neu.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FloatWritable  implements Writable,Comparable<FloatWritable>{
	private Float value;
	
	

	

	public FloatWritable(float value) {
		super();
		this.value = value;
	}

	public void write(DataOutput out) throws IOException {
	
		out.writeFloat(value);
	}

	

	public void readFields(DataInput in) throws IOException {
		
		value=in.readFloat();
	}
	
	public int compareTo(FloatWritable o) {
	
		return o.getValue() < this.getValue() ?-1
				:o.getValue()>this.getValue() ?  1: 
				0;	
	}
	public Float getValue() {
		return value;
	}

}
