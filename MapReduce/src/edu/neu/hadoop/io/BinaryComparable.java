package edu.neu.hadoop.io;

public abstract class BinaryComparable implements Comparable<BinaryComparable> {
	
	/**
	 * Return n st bytes 0..n-1 from {#getBytes()} are valid.
	 */
	public abstract int getLength();

	/**
	 * Return representative byte array for this instance.
	 */
	public abstract byte[] getBytes();
	
	/**
	 * Compare bytes from {#getBytes()}.
	 * 
	 * @see org.apache.hadoop.io.WritableComparator#compareBytes(byte[],int,int,byte[],int,int)
	 */
	@Override
	public int compareTo(BinaryComparable other) {
		if (this == other)
			return 0;
		return WritableComparator.compareBytes(getBytes(), 0, getLength(),
				other.getBytes(), 0, other.getLength());
	}
}
