package edu.neu.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Text extends BinaryComparable 
	implements WritableComparable<BinaryComparable> {

	@Override
	public void write(DataOutput out) throws IOException {
	}

	@Override
	public void readFields(DataInput in) throws IOException {
	}

	@Override
	public int compareTo(BinaryComparable arg0) {
		return 0;
	}

}
