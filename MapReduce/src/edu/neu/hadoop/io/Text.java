package edu.neu.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A serializable String object which implements a simple, efficient,
 * serialization protocol, based on {@link DataInput} and {@link DataOutput}
 * 
 * @author Rushikesh Badami
 */
public class Text implements Writable, Comparable<Text> {

	private String value;
	
	public Text() { }

	public Text(String value) {
		this.value = value;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeChars(value);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		value = in.readLine();
	}

	@Override
	public int compareTo(Text o) {
		return this.value.compareTo(o.getValue());
	}

	public String getValue() {
		return value;
	}
}
