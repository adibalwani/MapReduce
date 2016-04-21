package edu.neu.hadoop.mapreduce;

import java.util.List;

import edu.neu.hadoop.conf.Configuration;
import edu.neu.hadoop.mapreduce.master.Barrier;
import edu.neu.hadoop.mapreduce.network.HostNameManager;
import edu.neu.hadoop.mapreduce.network.ObjectSocket;

/**
 * Master node in the Hadoop cluster
 * 
 * @author Adib Alwani
 */
public class Master {
	
	private Configuration conf;
	
	@SuppressWarnings("unused")
	private Master() { }
	
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
		if (conf.getOutputPath().getPath().contains("s3")) {
			// Run on Cluster
			HostNameManager hostNameManager = new HostNameManager();
			List<String> workerNodes = hostNameManager.getWorkerNodes();
			runMapper(workerNodes);
			runReducer(workerNodes);
			return true;
		} else {
			// Run in Pseudo mode
			MapperThread mapTask = new MapperThread(conf);
			ReducerThread reduceTask = new ReducerThread(conf);
			try {
				mapTask.start();
				mapTask.join();
				reduceTask.start();
				reduceTask.join();
				return true;
			} catch (InterruptedException e) {
				return false;
			}
		}
		
	}
	
	/**
	 * Ping all worker nodes to start their Mapper Task and wait for its completion
	 * 
	 * @param workerNodes List of worker nodes
	 */
	private void runMapper(List<String> workerNodes) {
		try {
			Barrier barrier = new Barrier(Constants.MAPPER_PORT, workerNodes.size());
			barrier.start();
			for (String worker : workerNodes) {
				ObjectSocket conn = new ObjectSocket(worker, Constants.MAPPER_PORT);
				conn.write(conf);
				conn.close();
			}
			barrier.join();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Ping all worker nodes to start their Reducer Task and wait for its completion
	 * 
	 * @param workerNodes List of worker nodes
	 */
	private void runReducer(List<String> workerNodes) {
		try {
			Barrier barrier = new Barrier(Constants.REDUCER_PORT, workerNodes.size());
			barrier.start();
			for (String worker : workerNodes) {
				ObjectSocket conn = new ObjectSocket(worker, Constants.REDUCER_PORT);
				conn.write(conf);
				conn.close();
			}
			barrier.join();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
