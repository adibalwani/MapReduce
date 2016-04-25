package edu.neu.hadoop.mapreduce.worker;

import java.io.File;
import java.io.IOException;

import edu.neu.hadoop.conf.Configuration;
import edu.neu.hadoop.fs.Cleaner;
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
	public static Configuration configuration;
	
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

		private final HostNameManager hostNameManager;

		public ReduceTask(Configuration conf, HostNameManager hostNameManager) {
			configuration = conf;
			this.hostNameManager = hostNameManager;
		}

		@Override
		public void run() {
			try {
				String outputPath = configuration.getOutputPath().getPath();
				String bucketPath = configuration.getOutputPath().getBucketPath();
				int instanceNumber = hostNameManager.getOwnInstanceNumber(); 
				
				// Fetch partition data from S3
				Runtime runtime = Runtime.getRuntime();
				Process process = runtime.exec(
						Constants.s3ToPartition(bucketPath, instanceNumber));
				process.waitFor();
				
				// Start Reducer
				edu.neu.hadoop.mapreduce.ReducerThread mapTask = 
						new edu.neu.hadoop.mapreduce.ReducerThread(configuration);
				mapTask.start();
				mapTask.join();

				// Send output data to S3
				process = runtime.exec(Constants.outputToS3(outputPath));
				process.waitFor();

				// Acknowledge Master of task completion
				ObjectSocket conn = new ObjectSocket(
					hostNameManager.getMasterHostName(),
					Constants.REDUCER_PORT
				);
				conn.write(Long.valueOf(0));
				conn.close();
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				Cleaner cleaner = new Cleaner();
				cleaner.deleteDirectory(new File(Constants.OUTPUT_FOLDER_NAME));
			}
		}
	}
}
