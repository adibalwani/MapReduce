package edu.neu.hadoop.mapreduce;

/**
 * POJO to store key-value pair
 * 
 * @author Adib Alwani
 */
public class KeyValue<KEY, VALUE> implements Comparable<KEY> {
	KEY key;
	VALUE value;

	public KeyValue(KEY key, VALUE value) {
		this.key = key;
		this.value = value;
	}

	@Override
	public int compareTo(KEY o) {
		return key.compareTo(o);
	}
}
