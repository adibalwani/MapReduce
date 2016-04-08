package edu.neu.hadoop.mapreduce;

import edu.neu.hadoop.conf.Configuration;

public class MapperThread extends Thread {
	
	private Configuration conf;
	
	public MapperThread(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public void run() {
		MapTask mapTask = new MapTask(conf);
		mapTask.run();
		
	}
}
