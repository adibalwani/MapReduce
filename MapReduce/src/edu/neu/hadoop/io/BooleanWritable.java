package edu.neu.hadoop.io;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;

public class BooleanWritable implements Writable{
	private boolean value;
	
	public BooleanWritable() {
		super();
		// TODO Auto-generated constructor stub
	}

	public BooleanWritable(boolean value) {
		super();
		this.value = value;
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
	out.writeBoolean(value);	
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		value=in.readBoolean();
	}
/*public static void main(String args[]) throws IOException
{
	BooleanWritable booleanWritable=new BooleanWritable(true);
	}*/
	
}
