package edu.neu.hadoop.mapreduce.main;

import edu.neu.hadoop.mapreduce.Constants;
import edu.neu.hadoop.mapreduce.lib.input.Reader;



/**
 * Main class to run the Map-Reduce framework in Cluster mode
 * 
 * @author Adib Alwani
 */
public class Master implements Reader.ReadListener {
	
	private int numOfWorker = -1;
	
	public static void main(String args[]) {
		Master master = new Master();
		
		// Get Number of Worker Nodes
		Reader reader = new Reader();
		reader.readFileOrDirectory(Constants.DNS_FILE_NAME, master);
		
		// 
	}

	@Override
	public void onReadLine(String line) {
		numOfWorker++;
	}
}
