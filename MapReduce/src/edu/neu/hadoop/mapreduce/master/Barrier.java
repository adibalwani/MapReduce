package edu.neu.hadoop.mapreduce.master;

import java.io.IOException;

import edu.neu.hadoop.mapreduce.network.ObjectSocket;

/**
 * Class used for handling barriers
 * 
 * @author Adib Alwani
 */
public class Barrier extends Thread {
	
	final ObjectSocket.Server svr;
	int barrierCount;

	public Barrier(int port, int barrierCount) throws IOException {
		svr = new ObjectSocket.Server(port);
		this.barrierCount = barrierCount;
	}
	
	@Override
	public void run() {
		try {
			ObjectSocket conn;
			while (null != (conn = svr.accept())) {
				barrierCount--;
				conn.close();
				if (barrierCount <= 0) {
					break;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
