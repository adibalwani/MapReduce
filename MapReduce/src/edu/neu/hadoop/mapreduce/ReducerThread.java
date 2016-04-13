package edu.neu.hadoop.mapreduce;

import edu.neu.hadoop.conf.Configuration;

public class ReducerThread extends Thread {

	private Configuration conf;
	
	public ReducerThread(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public void run() {
		ReduceTask reduceTask = new ReduceTask(conf);
		reduceTask.run();
		
	}
}
