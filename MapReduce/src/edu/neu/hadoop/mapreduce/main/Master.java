package edu.neu.hadoop.mapreduce.main;

import edu.neu.hadoop.mapreduce.Constants;
import edu.neu.hadoop.mapreduce.master.MasterThread;


/**
 * Main class for Cluster's {@link Master} to run the Map-Reduce framework in Cluster mode
 * 
 * @author Adib Alwani
 */
public class Master {

	public static void main(String args[]) {
		try {
			MasterThread masterThread = new MasterThread(Constants.MASTER_PORT);
			masterThread.start();
			masterThread.join();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
