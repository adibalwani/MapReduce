package edu.neu.hadoop.mapreduce.worker;

import java.io.File;
import java.io.IOException;

import edu.neu.hadoop.conf.Configuration;
import edu.neu.hadoop.fs.Cleaner;
import edu.neu.hadoop.fs.Path;
import edu.neu.hadoop.mapreduce.Constants;
import edu.neu.hadoop.mapreduce.Counters;
import edu.neu.hadoop.mapreduce.Mapper;
import edu.neu.hadoop.mapreduce.network.HostNameManager;
import edu.neu.hadoop.mapreduce.network.ObjectSocket;


/**
 * Map task request listener in a worker node that runs the {@link Mapper} of the 
 * Map-Reduce framework upon request from the Master node in the cluster
 * 
 * @author Adib Alwani
 */
public class MapperThread extends Thread {
	
	private final ObjectSocket.Server svr;
	private final HostNameManager hostNameManager;
	public static Configuration configuration;
	
	public MapperThread(int port, HostNameManager hostNameManager) throws IOException {
		svr = new ObjectSocket.Server(port);
		this.hostNameManager = hostNameManager;
	}
	
	@Override
	public void run() {
		try {
			ObjectSocket conn;
			while (null != (conn = svr.accept())) {
				Counters.MAP_OUTPUT_RECORDS = 0;
				Configuration conf = (Configuration) conn.read();
				MapTask mapTask = new MapTask(conf, hostNameManager);
				mapTask.start();
				conn.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}		
	}
	
	/**
	 * Thread class to off-load map task on a thread
	 * 
	 * @author Adib Alwani
	 */
	private static class MapTask extends Thread {
		
		private final HostNameManager hostNameManager;
		
		public MapTask(Configuration conf, HostNameManager hostNameManager) {
			configuration = conf;
			configuration.setInputPath(new Path[] { new Path(Constants.INPUT_FOLDER_NAME) });
			configuration.setNumReduceTasks(hostNameManager.getWorkerNodes().size() + 1);
			this.hostNameManager = hostNameManager;
		}
		
		@Override
		public void run() {
			try {
				// Start Mapper
				edu.neu.hadoop.mapreduce.MapperThread mapTask = 
						new edu.neu.hadoop.mapreduce.MapperThread(configuration);
				mapTask.start();
				mapTask.join();
				
				// Send partition data to S3
				Runtime runtime = Runtime.getRuntime();
				Process process = runtime.exec(
						Constants.partitionToS3(configuration.getOutputPath().getBucketPath()));
				process.waitFor();
				
				// Acknowledge Master of task completion
				ObjectSocket conn = new ObjectSocket(
					hostNameManager.getMasterHostName(), 
					Constants.MAPPER_PORT
				);
				conn.write(Long.valueOf(Counters.MAP_OUTPUT_RECORDS));
				conn.close();
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				Cleaner cleaner = new Cleaner();
				cleaner.deleteDirectory(new File(Constants.PARTITION_FOLDER_NAME));
			}
		}
	}
}
