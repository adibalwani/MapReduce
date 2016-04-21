package edu.neu.hadoop.mapreduce.main;

import edu.neu.hadoop.mapreduce.Constants;
import edu.neu.hadoop.mapreduce.network.HostNameManager;
import edu.neu.hadoop.mapreduce.network.ObjectSocket;


/**
 * Main class for Client to run the Map-Reduce framework in Cluster mode
 * 
 * @author Adib Alwani
 */
public class Client {

	public static void main(String args[]) {
		if (args.length < 3) {
			System.out.println("Run as: Client [Program] [input path] [output path]");
			return;
		}
		
		HostNameManager hostNameManager = new HostNameManager();
		StringBuilder builder = new StringBuilder();
		builder.append(args[0]);
		builder.append(' ');
		builder.append(args[1]);
		builder.append(' ');
		builder.append(args[2]);
		
		try {
			ObjectSocket conn = new ObjectSocket(hostNameManager.getMasterHostName(), 
					Constants.MAPPER_PORT);
			conn.write(builder.toString());
			conn.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
