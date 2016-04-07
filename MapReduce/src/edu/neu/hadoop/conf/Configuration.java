package edu.neu.hadoop.conf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import edu.neu.hadoop.io.Writable;

public class Configuration 
	implements Iterable<Map.Entry<String,String>>, Writable {

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Iterator<Entry<String, String>> iterator() {
		// TODO Auto-generated method stub
		return null;
	}

}
