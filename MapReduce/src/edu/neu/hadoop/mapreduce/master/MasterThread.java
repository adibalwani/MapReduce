package edu.neu.hadoop.mapreduce.master;

import java.io.IOException;

import edu.neu.hadoop.conf.Configuration;
import edu.neu.hadoop.mapreduce.Mapper;
import edu.neu.hadoop.mapreduce.Reducer;
import edu.neu.hadoop.mapreduce.main.Hadoop;
import edu.neu.hadoop.mapreduce.network.ObjectSocket;


/**
 * Master Thread class that handles client's request.
 * <p>
 * Runs {@link Mapper} followed by {@link Reducer} on cluster's worker nodes.
 * Each worker node is passed a copy of {@link Configuration} object for the Job.
 * </p>
 * 
 * @author Adib Alwani
 */
public class MasterThread extends Thread {
	
	private ObjectSocket.Server svr;
	
	public MasterThread(int port) throws IOException {
		svr = new ObjectSocket.Server(port);
	}

	@Override
	public void run() {
		try {
			ObjectSocket conn;
			while (null != (conn = svr.accept())) {
				String args = (String) conn.read();
				Hadoop.main(args.split("\\s+"));
				System.out.println("Connection Terminated");
			}
			System.out.println("Master Terminated");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
