import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Class used for handling network connections i.e. sending and
 * receiving data
 * 
 * @author Adib Alwani, Bhavin Vora
 */
public class NetworkManager {
	
	private static final String DNS_FILE_NAME = "instance-dns";
	private static final int PORT_START = 10000;
	public static final int BARRIER_PORT_1 = 9998;
	public static final int BARRIER_PORT_2 = 9999;
	
	private List<HostPortPair> hostList;
	private HostPortPair hostPortPair;
	private List<List<TempDetails>> tempDetailsMasterList;
	private List<Partition> partitionList;
	private Barrier barrier;
	
	/*
	 * Map from client to opened server sockets for reuse  
	 */
	private Map<String, TextSocket.Server> serverSocketMap;
	private Map<String, TextSocket> clientSocketMap;
	
	public NetworkManager() {
		hostList = new ArrayList<HostPortPair>();
		readDNSFile();
		serverSocketMap = new HashMap<String, TextSocket.Server>();
		clientSocketMap = new HashMap<String, TextSocket>(); 
		partitionList = new ArrayList<Partition>();
	}
	
	/**
	 * Spawn Server thread to read incoming client connections
	 */
	public void spawnServers() {
		tempDetailsMasterList = new ArrayList<List<TempDetails>>();
		barrier = new Barrier(BARRIER_PORT_1);
		for (HostPortPair client : hostList) {
			// Don't spawn for self
			if (client.isHost()) {
				continue;
			}
			
			// Start server
			try {
				TextSocket.Server svr = new TextSocket.Server(
						getServerPort(client)
				);
				serverSocketMap.put(client.getHostName(), svr);
				List<TempDetails> listDetails = new ArrayList<TempDetails>(); 
				tempDetailsMasterList.add(listDetails);
				new Server(svr, listDetails).start();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Establish connections to all servers
	 */
	public void waitForServers() {
		for (HostPortPair server : hostList) {
			// Don't establish connection to self
			if (server.isHost()) {
				continue;
			}
			
			while (true) {
				try {
					String serverHostName = server.getHostName();
					TextSocket conn = new TextSocket(serverHostName, getClientPort(server));
					clientSocketMap.put(serverHostName, conn);
					//conn.close();
					break;
				} catch (IOException e) {
					System.out.println("Waiting for " + server.getHostName() + " to be up");
					try {
						Thread.sleep(100);
					} catch (InterruptedException e1) {
						e1.printStackTrace();
					}
				}
			}
		}
	}
	
	/**
	 * Sort the data on the cluster
	 * 
	 * @param tempDetails Input data
	 */
	public void sortData(List<TempDetails> tempDetails) {
		List<TempDetails> dataList = new ArrayList<TempDetails>();
		Barrier barrier = new Barrier(BARRIER_PORT_2);
		
		for (TempDetails tempDetail : tempDetails) {
			// Send data to appropriate servers
			String hostName = getHostName(tempDetail.getTemperature());
			
			// Don't send to self
			if (hostPortPair.hostName.equals(hostName)) {
				dataList.add(tempDetail);
				continue;
			}
			
			try {
				HostPortPair server = getHostPortPair(hostName);
				TextSocket conn = clientSocketMap.get(server.getHostName());
				conn.putln(tempDetail.toString());
				//TextSocket conn = new TextSocket(hostName, getClientPort(server));
				//new Client(conn, tempDetail.toString()).run();
				//conn.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		for (HostPortPair server : hostList) {
			// Don't send to itself
			if (server.isHost()) {
				continue;
			}
			new Barrier.CompleteThread(server.getHostName(), BARRIER_PORT_2).start();
		}
		
		// Wait for barrier to lift
		barrier.barrier(hostList.size());
		System.out.println("Barrier lifted");
		
		
		List<TempDetails> data = mergeList(dataList);
		Collections.sort(data);
		if (data.size() != 0) {
			Printer.printTempDetails(data, "output/" + 
					getTemperature(hostPortPair.getHostName(), data.get(0).getTemperature()));
		}
	}
	
	/**
	 * Broadcast sample data to all servers and fill the partitioner
	 * 
	 * @param tempDetails Sample data
	 */
	public void fillPartition(List<TempDetails> tempDetails) {
		int len = hostList.size();
		
		for (HostPortPair server : hostList) {
			// Don't broadcast to self
			if (server.isHost()) {
				continue;
			}
			
			// Broadcast samples to servers
			try {
				TextSocket conn = clientSocketMap.get(server.getHostName());
				//TextSocket conn = new TextSocket(server.getHostName(), getClientPort(server));
				for (TempDetails tempDetail : tempDetails) {
					conn.putln(tempDetail.toString());
				}
				//conn.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			// Broadcast barrier complete
			new Barrier.CompleteThread(server.getHostName(), BARRIER_PORT_1).start(); 
		}
		
		// Wait for barrier to lift
		barrier.barrier(len);
		System.out.println("Barrier lifted");
		
		// Fill the pivots
		List<TempDetails> samples = mergeList(tempDetails);
		Collections.sort(samples);
		Collections.reverse(samples);
		getPivots(samples);
		Printer.printTempDetails(samples, "file");
		clearList();
	}
	
	/**
	 * Get the host port pair instance corresponding to this host
	 * 
	 * @param hostName Host Name to get instance of
	 * @return Instance of host port pair
	 */
	private HostPortPair getHostPortPair(String hostName) {
		for (HostPortPair hostPortPair : hostList) {
			if (hostPortPair.getHostName().equals(hostName)) {
				return hostPortPair;
			}
		}
		
		return null;
	}
	
	/**
	 * Clear all the data Lists
	 */
	private void clearList() {
		for (List<TempDetails> tempDetails : tempDetailsMasterList) {
			tempDetails.clear();
		}
	}
	
	/**
	 * Fill the partitions list
	 * 
	 * @param samples Samples to partition on
	 */
	private void getPivots(List<TempDetails> samples) {
		int len = hostList.size();
		int index = samples.size() / len;
		
		for (int i = 0; i < len - 1; i++) {
			TempDetails tempDetails = samples.get(index);
			float temp = tempDetails.getTemperature();
			String hostName = hostList.get(i).getHostName();
			partitionList.add(new Partition(temp, hostName));
			System.out.println("Pivot: "  + temp + " " + hostName);
			index += index;
		}
	}
	
	/**
	 * Find the appropriate host for the given temperature
	 * 
	 * @param temperature The temperature to search for
	 * @return Host Name that will handle the partition
	 */
	private String getHostName(float temperature) {
		for (Partition partition : partitionList) {
			if (temperature <= partition.getMaxTemp()) {
				return partition.getHostName();
			}
		}
		
		return hostList.get(hostList.size() - 1).getHostName();
	}

	/**
	 * Get the temperature partition for the given host
	 * 
	 * @param hostName Host Name
	 * @param defaultValue Last host temperature
	 * @return Temperature partition
	 */
	private float getTemperature(String hostName, float defaultValue) {
		for (Partition partition : partitionList) {
			if (partition.getHostName().equals(hostName)) {
				return partition.getMaxTemp();
			}
		}
		
		return defaultValue;
	}
	
	/**
	 * Merge master list and the given list to one
	 * 
	 * @param tempDetails List to merge
	 * @return Merged list
	 */
	private List<TempDetails> mergeList(List<TempDetails> tempDetails) {
		List<TempDetails> samples = new ArrayList<TempDetails>();
		for (List<TempDetails> tempDetail : tempDetailsMasterList) {
			samples.addAll(tempDetail);
		}
		samples.addAll(tempDetails);
		return samples;
	}
	
	/**
	 * Close all established connections - Server
	 */
	public void closeConnections() {
		// Close server connections
		for (Map.Entry<String, TextSocket.Server> entry : serverSocketMap.entrySet()) {
			try {
				entry.getValue().server.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Read the DNS and fill the DNS List
	 */
	private void readDNSFile() {
		try (
			FileReader fileReader = new FileReader(DNS_FILE_NAME);
			BufferedReader reader = new BufferedReader(fileReader);
		) {
			String currentLine;
			int instanceNumber = 1;
				
			while ((currentLine = reader.readLine()) != null) {
				String[] line = currentLine.split("\\s+");
				String dns = line[0];
				boolean host = false;
				
				if (line.length == 2) {
					host = true;
					hostPortPair = new HostPortPair(dns, instanceNumber, host);
				}
				
				hostList.add(new HostPortPair(dns, instanceNumber, host));
				instanceNumber++;
			}
		} catch (IOException exception) {
			exception.printStackTrace();
		}
	}
	
	/**
	 * Generate the port to be used as a server for the given receiver
	 * 
	 * @param receiver Receiver DNSPort pair
	 * @return Port number
	 */
	private int getServerPort(HostPortPair receiver) {
		int len = hostList.size();
		return (hostPortPair.getInstanceNumber() - 1) * 2 + 
				(receiver.getInstanceNumber() - 1) +
				(len * len - len) + PORT_START;
	}
	
	/**
	 * Generate the port to be used as a client for the given receiver
	 * 
	 * @param receiver Receiver DNSPort pair
	 * @return Port number
	 */
	private int getClientPort(HostPortPair receiver) {
		int len = hostList.size();
		return (receiver.getInstanceNumber() - 1) * 2 + 
				(hostPortPair.getInstanceNumber() - 1) +
				(len * len - len) + PORT_START;
	}
	
	/**
	 * Class representing temperature and HostName
	 * 
	 * @author Adib Alwani, Bhavin Vora 
	 */
	static class Partition {
		private final float maxTemp;
		private final String hostName;
		
		public Partition(float maxTemp, String hostName) {
			this.maxTemp = maxTemp;
			this.hostName = hostName;
		}

		public float getMaxTemp() {
			return maxTemp;
		}

		public String getHostName() {
			return hostName;
		}
	}

	/**
	 * Class representing DNS and Port pair
	 * 
	 * @author Adib Alwani, Bhavin Vora
	 */
	static class HostPortPair {
		private final String hostName;
		private final int instanceNumber;
		private final boolean host;
		
		public HostPortPair(String dns, int instanceNumber, boolean host) {
			this.hostName = dns;
			this.instanceNumber = instanceNumber;
			this.host = host;
		}

		public String getHostName() {
			return hostName;
		}

		public int getInstanceNumber() {
			return instanceNumber;
		}

		public boolean isHost() {
			return host;
		}
	}
}
