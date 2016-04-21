package edu.neu.hadoop.mapreduce.master;

import java.io.IOException;

import edu.neu.hadoop.mapreduce.Constants;
import edu.neu.hadoop.mapreduce.network.TextSocket;


/**
 * Master Thread class that for handling client's request 
 * 
 * @author Adib Alwani
 */
public class MasterThread extends Thread {
	
	private final TextSocket.Server svr;
	private final int numOfWorker;
	
	public MasterThread(int port, int numOfWorker) throws IOException {
		svr = new TextSocket.Server(port);
		this.numOfWorker = numOfWorker;
	}

	@Override
	public void run() {
		try {
			TextSocket conn;
			while (null != (conn = svr.accept())) {
				// TODO: Spawns Mapper/Reducer thread and waits for completion
				Barrier barrier = new Barrier(Constants.MAPPER_PORT, numOfWorker);
				barrier.start();
				barrier.join();
				
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
