package edu.neu.hadoop.mapreduce;

import edu.neu.hadoop.conf.Configuration;
import edu.neu.hadoop.mapreduce.lib.input.Reader;

/**
 * Thread class to spawn a new {@link Reducer} Implementation.
 * 
 * @author Adib Alwani
 */
@SuppressWarnings("rawtypes")
public class ReducerThread extends Thread {

	private Configuration conf;
	private Class<? extends Reducer> reducerClass;
	
	public ReducerThread(Configuration conf) {
		this.conf = conf;
		this.reducerClass = conf.getReducerClass();
	}

	@Override
	public void run() {
		System.out.println("Reducer Started");
		Reader reader = new Reader();
		reader.read(0, "Thread-0", conf);
		System.out.println("Reducer Ended");
	}
}
