package edu.neu.hadoop.mapreduce;

import edu.neu.hadoop.io.BooleanWritable;
import edu.neu.hadoop.io.FloatWritable;
import edu.neu.hadoop.io.IntWritable;
import edu.neu.hadoop.io.Text;


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
		try {
			if (key instanceof Text) {
				this.key = (KEY) ((Text) key).clone();
			} else if (key instanceof IntWritable) {
				this.key = (KEY) ((IntWritable) key).clone();
			} else if (key instanceof FloatWritable) {
				this.key = (KEY) ((FloatWritable) key).clone();
			} else if (key instanceof BooleanWritable) {
				this.key = (KEY) ((BooleanWritable) key).clone();
			} else {
				this.key = key;
			}
			
			if (value instanceof Text) {
				this.value = (VALUE) ((Text) value).clone();
			} else if (value instanceof IntWritable) {
				this.value = (VALUE) ((IntWritable) value).clone();
			} else if (value instanceof FloatWritable) {
				this.value = (VALUE) ((FloatWritable) value).clone();
			} else if (value instanceof BooleanWritable) {
				this.value = (VALUE) ((BooleanWritable) value).clone();
			} else {
				this.value = value;
			}
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
