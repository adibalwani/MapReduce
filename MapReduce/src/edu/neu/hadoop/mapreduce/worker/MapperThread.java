package edu.neu.hadoop.mapreduce.worker;

import java.io.IOException;

import edu.neu.hadoop.mapreduce.network.TextSocket;


/**
 * 
 * 
 * @author Adib Alwani
 */
public class MapperThread extends Thread {
	
	private final TextSocket.Server svr;
	
	public MapperThread(int port) throws IOException {
		svr = new TextSocket.Server(port);
	}
	
	@Override
	public void run() {
		try {
			TextSocket conn;
			while (null != (conn = svr.accept())) { 
				// TODO: Do actual mapper task
			}
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}
}
