package edu.neu.hadoop.mapreduce.worker;

import java.io.IOException;

import edu.neu.hadoop.conf.Configuration;
import edu.neu.hadoop.mapreduce.Constants;
import edu.neu.hadoop.mapreduce.Reducer;
import edu.neu.hadoop.mapreduce.network.HostNameManager;
import edu.neu.hadoop.mapreduce.network.ObjectSocket;


/**
 * Reduce task request listener in a worker node that runs the {@link Reducer} of the 
 * Map-Reduce framework upon request from the Master node in the cluster
 * 
 * @author Adib Alwani
 */
public class ReducerThread extends Thread {
	
	private final ObjectSocket.Server svr;
	private final HostNameManager hostNameManager;
	
	public ReducerThread(int port, HostNameManager hostNameManager) throws IOException {
		svr = new ObjectSocket.Server(port);
		this.hostNameManager = hostNameManager;
	}
	
	@Override
	public void run() {
		try {
			ObjectSocket conn;
			while (null != (conn = svr.accept())) {
				Configuration conf = (Configuration) conn.read();
				ReduceTask reduceTask = new ReduceTask(conf, hostNameManager);
				reduceTask.start();
				conn.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}		
	}
	
	/**
	 * Thread class to off-load reduce task on a thread
	 * 
	 * @author Adib Alwani
	 */
	private static class ReduceTask extends Thread {

		private final Configuration conf;
		private final HostNameManager hostNameManager;

		public ReduceTask(Configuration conf, HostNameManager hostNameManager) {
			this.conf = conf;
			this.hostNameManager = hostNameManager;
		}

		@Override
		public void run() {
			try {
				String outputPath = conf.getOutputPath().getPath();
				String bucketPath = conf.getOutputPath().getBucketPath();
				int instanceNumber = hostNameManager.getOwnInstanceNumber(); 
				
				// Fetch partition data from S3
				Runtime runtime = Runtime.getRuntime();
				Process process = runtime.exec(
						Constants.s3ToPartition(bucketPath, instanceNumber));
				process.waitFor();
				
				// Start Reducer
				edu.neu.hadoop.mapreduce.ReducerThread mapTask = 
						new edu.neu.hadoop.mapreduce.ReducerThread(conf);
				mapTask.start();
				mapTask.join();

				// Send output data to S3
				process = runtime.exec(Constants.outputToS3(bucketPath, outputPath));
				process.waitFor();

				// Acknowledge Master of task completion
				ObjectSocket conn = new ObjectSocket(
					hostNameManager.getMasterHostName(),
					Constants.REDUCER_PORT
				);
				conn.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
