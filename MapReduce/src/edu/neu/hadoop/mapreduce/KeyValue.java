package edu.neu.hadoop.mapreduce;


/**
 * POJO to store key-value pair
 * 
 * @author Adib Alwani
 */
@SuppressWarnings({ "unchecked" })
public class KeyValue<KEY, VALUE> implements Comparable<KeyValue<KEY, VALUE>> {
	
	private KEY key;
	private VALUE value;

	public KeyValue(KEY key, VALUE value) {
		this.key = key;
		this.value = value;
	}

	@Override
	public int compareTo(KeyValue<KEY, VALUE> o) {
		return ((Comparable<KEY>) key).compareTo(o.getKey());
	}

	public KEY getKey() {
		return key;
	}

	public VALUE getValue() {
		return value;
	}
}
