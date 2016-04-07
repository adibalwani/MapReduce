package edu.neu.hadoop.io;

import java.io.IOException;

import edu.neu.hadoop.conf.Configurable;
import edu.neu.hadoop.conf.Configuration;

public class WritableComparator implements RawComparator, Configurable {
	
	private Configuration conf;
	private final WritableComparable key1;
	private final WritableComparable key2;
	private final DataInputBuffer buffer;

	@Override
	public int compare(Object a, Object b) {
		return compare((WritableComparable)a, (WritableComparable)b);
	}

	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		try {
			buffer.reset(b1, s1, l1); // parse key1
			key1.readFields(buffer);

			buffer.reset(b2, s2, l2); // parse key2
			key2.readFields(buffer);

			buffer.reset(null, 0, 0); // clean up reference
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		return compare(key1, key2); // compare them
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public Configuration getConf() {
		return conf;
	}
	
	/** Lexicographic order of binary data. */
	public static int compareBytes(byte[] b1, int s1, int l1, byte[] b2,
			int s2, int l2) {
		return FastByteComparisons.compareTo(b1, s1, l1, b2, s2, l2);
	}

}
