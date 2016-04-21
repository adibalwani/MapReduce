package edu.neu.hadoop.mapreduce.network;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import edu.neu.hadoop.mapreduce.Constants;


/**
 * Class used for managing Host names across the network of clusters 
 * 
 * @author Adib Alwani
 */
public class HostNameManager {
	
	private List<String> workerNodes;
	private String ownHostName;
	private String masterHostName;
	
	public HostNameManager() {
		workerNodes = new ArrayList<String>();
		readDNSFile();
	}
	
	/**
	 * Read the DNS and fill the DNS List
	 */
	private void readDNSFile() {
		try (
			FileReader fileReader = new FileReader(Constants.DNS_FILE_NAME);
			BufferedReader reader = new BufferedReader(fileReader);
		) {
			String currentLine;
				
			while ((currentLine = reader.readLine()) != null) {
				String[] line = currentLine.split("\\s+");
				String dns = line[0];
				
				// Master is the first node in the cluster
				if (masterHostName == null) {
					masterHostName = dns;
				} else if (line.length == 2) {
					ownHostName = dns;
				} else {
					workerNodes.add(dns);
				}
			}
		} catch (IOException exception) {
			exception.printStackTrace();
		}
	}

	public List<String> getWorkerNodes() {
		return workerNodes;
	}

	public String getOwnHostName() {
		return ownHostName;
	}

	public String getMasterHostName() {
		return masterHostName;
	}
}
