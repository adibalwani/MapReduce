package edu.neu.hadoop.mapreduce.master;

import java.io.IOException;

import edu.neu.hadoop.mapreduce.network.TextSocket;

/**
 * Class used for handling barriers
 * 
 * @author Adib Alwani
 */
public class Barrier extends Thread {
	
	final TextSocket.Server svr;
	int barrierCount;

	public Barrier(int port, int barrierCount) throws IOException {
		svr = new TextSocket.Server(port);
		this.barrierCount = barrierCount;
	}
	
	@Override
	public void run() {
		try {
			TextSocket conn;
			while (null != (conn = svr.accept())) {
				barrierCount--;
				if (barrierCount <= 0) {
					break;
				}
				conn.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
