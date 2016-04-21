package edu.neu.hadoop.mapreduce.worker;

import java.io.IOException;

import edu.neu.hadoop.conf.Configuration;
import edu.neu.hadoop.fs.Path;
import edu.neu.hadoop.mapreduce.Constants;
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
	
	public MapperThread(int port, HostNameManager hostNameManager) throws IOException {
		svr = new ObjectSocket.Server(port);
		this.hostNameManager = hostNameManager;
	}
	
	@Override
	public void run() {
		try {
			ObjectSocket conn;
			while (null != (conn = svr.accept())) {
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
		
		private final Configuration conf;
		private final HostNameManager hostNameManager;
		
		public MapTask(Configuration conf, HostNameManager hostNameManager) {
			this.conf = conf;
			this.conf.setInputPath(new Path[] { new Path(Constants.INPUT_FOLDER_NAME) });
			this.hostNameManager = hostNameManager;
		}
		
		@Override
		public void run() {
			try {
				// Start Mapper
				edu.neu.hadoop.mapreduce.MapperThread mapTask = 
						new edu.neu.hadoop.mapreduce.MapperThread(conf);
				mapTask.start();
				mapTask.join();
				
				// Send data to S3
				Runtime runtime = Runtime.getRuntime();
				Process process = runtime.exec(
					Constants.PARTITION_TO_S3_COMMAND_1 + 
					conf.getOutputPath().getPath() + 
					Constants.PARTITION_TO_S3_COMMAND_2
				);
				process.waitFor();
				
				// Acknowledge Master of task completion
				ObjectSocket conn = new ObjectSocket(
					hostNameManager.getMasterHostName(), 
					Constants.MAPPER_PORT
				);
				conn.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
