package edu.neu.hadoop.mapreduce;

import edu.neu.hadoop.io.Writable;


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
		Writable writableKey = (Writable) key;
		Writable writableValue = (Writable) value;
		try {
			this.key = (KEY) writableKey.clone();
			this.value = (VALUE) writableValue.clone();
		} catch (CloneNotSupportedException e) {
			e.printStackTrace();
		}
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
