import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.swing.text.MaskFormatter;


/**
 * Class used for handling network connections i.e. sending and
 * receiving data
 * 
 * @author Adib Alwani, Bhavin Vora
 */
public class NetworkManager {
	
	private static final String DNS_FILE_NAME = "instance-dns";
	private static final int PORT_START = 10000;
	
	private List<HostPortPair> hostList;
	private HostPortPair hostPortPair;
	private NetworkManager networkManager;
	private List<List<TempDetails>> tempDetailsMasterList;
	private List<Partition> partitionList;
	
	/*
	 * Map from Server DNS to opened socket for reuse
	 */
	private Map<String, TextSocket> clientSocketMap;
	
	/*
	 * Map from client to opened server sockets for reuse  
	 */
	private Map<String, TextSocket.Server> serverSocketMap;
	
	private NetworkManager() { }
	
	/**
	 * Class used for getting an instance of Network Manager
	 * 
	 * @return NetworkManager Instance
	 */
	public static NetworkManager newInstance() {
		NetworkManager networkManager = new NetworkManager();
		networkManager.hostList = new ArrayList<HostPortPair>();
		networkManager.readDNSFile();
		networkManager.networkManager = networkManager;
		networkManager.clientSocketMap = new HashMap<String, TextSocket>();
		networkManager.serverSocketMap = new HashMap<String, TextSocket.Server>();
		networkManager.partitionList = new ArrayList<Partition>(); 
		return networkManager;
	}
	
	/**
	 * Spawn Server thread to read incoming client connections
	 */
	public void spawnServers() {
		tempDetailsMasterList = new ArrayList<List<TempDetails>>();
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
				networkManager.new Server(svr, listDetails).start();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		// Add itself
		List<TempDetails> listDetails = new ArrayList<TempDetails>(); 
		tempDetailsMasterList.add(listDetails);
	}
	
	/**
	 * Establish connections to all servers
	 */
	public void spawnClients() {
		for (HostPortPair server : hostList) {
			// Don't establish connection to self
			if (server.isHost()) {
				continue;
			}
			
			try {
				String serverHostName = server.getHostName();
				TextSocket conn = new TextSocket(serverHostName, getClientPort(server));				
				clientSocketMap.put(serverHostName, conn);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Sort the data on the cluster
	 * 
	 * @param tempDetails Input data
	 */
	public void sortData(List<TempDetails> tempDetails) {
		Barrier barrier = new Barrier();
		
		for (TempDetails tempDetail : tempDetails) {
			// Send data to appropriate servers
			String hostName = getHostName(tempDetail.getTemperature());
			
			// Don't send to self
			if ()
			
			TextSocket conn = clientSocketMap.get(hostName);
			networkManager.new Client(conn, tempDetail.toString()).start();
		}
		
		for (int i = 0; i < len; i++) {
			HostPortPair server = hostList.get(i);
			
			// Don't broadcast to self
			if (server.isHost()) {
				continue;
			}
			
			// Send data to appropriate servers
			TextSocket conn = clientSocketMap.get(server.getHostName());
			for (TempDetails tempDetail : tempDetails) {
				networkManager.new Client(conn, tempDetail.toString()).start();
			}
		}
		
		for (HostPortPair server : hostList) {
			new Barrier.CompleteThread(server.getHostName()).start();
		}
		
		// Wait for barrier to lift
		barrier.barrier(len);
	}
	
	/**
	 * Broadcast sample data to all servers and fill the partitioner
	 * 
	 * @param tempDetails Sample data
	 */
	public void fillPartition(List<TempDetails> tempDetails) {
		Barrier barrier = new Barrier();
		int len = hostList.size();
		
		for (int i = 0; i < len; i++) {
			HostPortPair server = hostList.get(i);
			
			// Don't broadcast to self
			// Add to list
			if (server.isHost()) {
				List<TempDetails> listDetails = tempDetailsMasterList.get(
						tempDetailsMasterList.size() - 1
				);
				for (TempDetails tempDetail : tempDetails) {
					listDetails.add(tempDetail);
				}
				continue;
			}
			
			// Broadcast samples to servers
			TextSocket conn = clientSocketMap.get(server.getHostName());
			for (TempDetails tempDetail : tempDetails) {
				networkManager.new Client(conn, tempDetail.toString()).start();
			}
			
			// Broadcast barrier complete
			new Barrier.CompleteThread(server.getHostName()).start(); 
		}
		
		// Wait for barrier to lift
		barrier.barrier(len);
		
		// Fill the pivots
		List<TempDetails> samples = mergeList();
		Collections.sort(samples);
		getPivots(samples);
		clearList();
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
	 * Merge master list into one
	 * 
	 * @return samples of the data
	 */
	private List<TempDetails> mergeList() {
		List<TempDetails> samples = new ArrayList<TempDetails>();
		for (List<TempDetails> tempDetail : tempDetailsMasterList) {
			samples.addAll(tempDetail);
		}
		return samples;
	}
	
	/**
	 * Close all established connections - Server, Client
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
		
		// Close client connections
		for (Map.Entry<String, TextSocket> entry : clientSocketMap.entrySet()) {
			try {
				entry.getValue().close();
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
		return (receiver.getInstanceNumber() - 1) * 2 + 
				(hostPortPair.getInstanceNumber() - 1) +
				(len * len - len) + PORT_START;
	}
	
	/**
	 * Generate the port to be used as a client for the given receiver
	 * 
	 * @param receiver Receiver DNSPort pair
	 * @return Port number
	 */
	private int getClientPort(HostPortPair receiver) {
		return (hostPortPair.getInstanceNumber() - 1) * 2 + 
				(receiver.getInstanceNumber() - 1) + PORT_START;
	}
	
	/**
	 * Thread to send a message to server
	 * 
	 * @author Adib Alwani
	 */
	class Client extends Thread {
		
		final String message; 
		final TextSocket conn;
		
		public Client(TextSocket conn, String message) {
			this.message = message;
			this.conn = conn;
		}
		
		@Override
		public void run() {
			try {
				conn.putln(message);
			} catch (IOException exception) {
				exception.printStackTrace();
			}
		}
	}
	
	/**
	 * Thread to spawn a server for the given client DNSPort pair
	 * 
	 * @author Adib Alwani
	 */
	class Server extends Thread {
		
		final TextSocket.Server svr;
		final List<TempDetails> listDetails;
		
		public Server(TextSocket.Server svr, List<TempDetails> listDetails) {
			this.svr = svr;
			this.listDetails = listDetails;
		}
		
		@Override
		public void run() {
			try {
				TextSocket conn;
		        while (null != (conn = svr.accept())) {
		            ServerThread tt = new ServerThread(conn, new ServerThread.DismissListener() {
						@Override
						public void onDismiss(TempDetails tempDetails) {
							synchronized (listDetails) {
								listDetails.add(tempDetails);
							}
						}
					});
		            tt.start();
		        }
			} catch (IOException exception) {
				exception.printStackTrace();
			}
		}
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
