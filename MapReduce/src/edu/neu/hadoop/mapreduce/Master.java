package edu.neu.hadoop.mapreduce;

import edu.neu.hadoop.conf.Configuration;

/**
 * Master node in the Hadoop cluster
 * 
 * @author Adib Alwani
 */
public class Master {
	
	private Configuration conf;
	
	@SuppressWarnings("unused")
	private Master() {
		conf = new Configuration();
	}
	
	public Master(Configuration conf) {
		this.conf = conf;
	}
	
	/**
	 * Start a job with the provided configurations
	 * 
	 * @return true, iff the job was successfully executed. False, otherwise
	 */
	public boolean submitJob() {
		System.out.println("Master Started");
		MapperThread mapTask = new MapperThread(conf);
		ReducerThread reduceTask = new ReducerThread(conf);
		mapTask.start();
		try {
			mapTask.join();
			reduceTask.start();
			reduceTask.join();
			return true;
		} catch (InterruptedException e) {
			return false;
		}
	}
}
