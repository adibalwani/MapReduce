package edu.neu.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A serializable byte object which implements a simple, efficient,
 * serialization protocol, based on {@link DataInput} and {@link DataOutput}
 * 
 * @author Rushikesh Badami
 * @modified Adib Alwani
 */
public class BytesWritable implements Writable, Cloneable, Comparable<BytesWritable> {

	private byte value;
	
	public BytesWritable() { }

	public BytesWritable(byte value) {
		this.value = value;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeByte(value);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		value = in.readByte();
	}

	@Override
	public int compareTo(BytesWritable o) {
		return 0;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Writable) {
			BytesWritable o = (BytesWritable) obj;
			return o.value == this.value;
		}
		return super.equals(obj);
	}
	
	@Override
	public Object clone() throws CloneNotSupportedException {
		return new BytesWritable(value); 
	}

	public byte get() {
		return value;
	}

	public void set(byte value) {
		this.value = value;
	}
}
