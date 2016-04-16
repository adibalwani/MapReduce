package edu.neu.hadoop.mapreduce;

import edu.neu.hadoop.conf.Configuration;

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
		try {
			Merger merger = new Merger(conf);
			merger.merge(0, Constants.MERGED_FILE_NAME);
			ReduceContext reduceContext = new ReduceContext(conf);
			Reducer reducer = (Reducer) Class.forName(reducerClass.getName()).newInstance();
			reducer.run(reduceContext);
			reduceContext.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("Reducer Ended");
	}
}
