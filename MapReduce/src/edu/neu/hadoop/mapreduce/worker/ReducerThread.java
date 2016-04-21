package edu.neu.hadoop.mapreduce.worker;

import java.io.IOException;

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
				// TODO: Do actual reducer task
			}
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}
}
