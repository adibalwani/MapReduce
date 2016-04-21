package edu.neu.hadoop.mapreduce.main;

import edu.neu.hadoop.mapreduce.Constants;
import edu.neu.hadoop.mapreduce.network.HostNameManager;
import edu.neu.hadoop.mapreduce.worker.MapperThread;
import edu.neu.hadoop.mapreduce.worker.ReducerThread;



/**
 * Main class for Cluster's {@link Worker} to run the Map-Reduce framework in Cluster mode
 * 
 * @author Adib Alwani
 */
public class Worker {

	public static void main(String args[]) {
		HostNameManager hostNameManager = new HostNameManager();
		try {
			MapperThread mapperThread = 
					new MapperThread(Constants.MAPPER_PORT, hostNameManager);
			ReducerThread reducerThread = 
					new ReducerThread(Constants.REDUCER_PORT, hostNameManager);
			mapperThread.start();
			reducerThread.start();
			mapperThread.join();
			reducerThread.join();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
