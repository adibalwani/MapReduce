package edu.neu.hadoop.mapreduce;

import edu.neu.hadoop.conf.Configuration;
import edu.neu.hadoop.mapreduce.network.HostNameManager;

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
		HostNameManager hostNameManager = new HostNameManager();
		try {
			Merger merger = new Merger(conf);
			boolean merge = merger.merge(hostNameManager.getOwnInstanceNumber(), 
					Constants.MERGED_FILE_NAME);
			if (merge) {
				ReduceContext reduceContext = new ReduceContext(conf);
				Reducer reducer = (Reducer) Class.forName(reducerClass.getName()).newInstance();
				reducer.run(reduceContext);
				reduceContext.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("Reducer Ended");
	}
}
