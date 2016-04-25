package edu.neu.hadoop.mapreduce.master;

import java.io.IOException;

import edu.neu.hadoop.mapreduce.Counters;
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
				Counters.MAP_OUTPUT_RECORDS += (Long) conn.read();
				conn.close();
				if (barrierCount <= 0) {
					System.out.println("MAP_OUTPUT_RECORDS " + Counters.MAP_OUTPUT_RECORDS);
					break;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				svr.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
