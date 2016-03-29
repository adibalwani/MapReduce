import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
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
	
	private List<DNSPortPair> dnsList;
	private DNSPortPair hostDNSPortPair;
	private NetworkManager networkManager;
	
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
		networkManager.dnsList = new ArrayList<DNSPortPair>();
		networkManager.readDNSFile();
		networkManager.networkManager = networkManager;
		networkManager.clientSocketMap = new HashMap<String, TextSocket>();
		networkManager.serverSocketMap = new HashMap<String, TextSocket.Server>();
		return networkManager;
	}
	
	/**
	 * Spawn Server thread to read incoming client connections
	 */
	public void spawnServers() {
		for (DNSPortPair client : dnsList) {
			// Don't spawn for self
			if (client.isHost()) {
				continue;
			}
			
			// Start server
			try {
				TextSocket.Server svr = new TextSocket.Server(
						getServerPort(client)
				);
				serverSocketMap.put(client.getDns(), svr);
				networkManager.new Server(svr).start();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Establish connections to all servers
	 */
	public void spawnClients() {
		for (DNSPortPair server : dnsList) {
			// Don't establish connection to self
			if (server.isHost()) {
				continue;
			}
			
			try {
				String serverDNS = server.getDns();
				TextSocket conn = new TextSocket(serverDNS, getClientPort(server));				
				clientSocketMap.put(serverDNS, conn);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Broadcast sample data to all servers
	 * 
	 * @param tempDetails Sample data
	 */
	public void broadcastSamples(List<TempDetails> tempDetails) {
		for (DNSPortPair server : dnsList) {
			// Don't broadcast to self
			if (server.isHost()) {
				continue;
			}
			
			// Broadcast samples to servers
			TextSocket conn = clientSocketMap.get(server.getDns());
			
			// TODO: Implement TempDetails toString()
			// TODO: For each List value send the data
			// TODO: Combine such value into one message and send data
		//	ArrayList<String>tempToString= convertListToString(tempDetails);
			for(TempDetails temp:tempDetails)
			{
			Client sendToServer=networkManager.new Client(conn,temp.toString());
			sendToServer.start();
			}
		}
	}
	
	/**
	 * Convert the object list of tempDetails to string
	 */
	/*public ArrayList<String> convertListToString(List<TempDetails> tempDetails)
	{
		ArrayList<String>listToString=new ArrayList<>();
		for (TempDetails temp:tempDetails)
		{
			listToString.add(temp.getwBan()+","+temp.getDate()+","+temp.getTime()+","+temp.getTemperature());
		}
			
		
		return listToString;
		
	}*/
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
					hostDNSPortPair = new DNSPortPair(dns, instanceNumber, host);
				}
				
				dnsList.add(new DNSPortPair(dns, instanceNumber, host));
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
	private int getServerPort(DNSPortPair receiver) {
		int len = dnsList.size();
		return (receiver.getInstanceNumber() - 1) * 2 + 
				(hostDNSPortPair.getInstanceNumber() - 1) +
				(len * len - len) + PORT_START;
	}
	
	/**
	 * Generate the port to be used as a client for the given receiver
	 * 
	 * @param receiver Receiver DNSPort pair
	 * @return Port number
	 */
	private int getClientPort(DNSPortPair receiver) {
		return (hostDNSPortPair.getInstanceNumber() - 1) * 2 + 
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
		
		public Server(TextSocket.Server svr) {
			this.svr = svr;
		}
		
		@Override
		public void run() {
			try {
				TextSocket conn;
				// Wait for incoming data from this client
		        while (null != (conn = svr.accept())) {
		            ServerThread tt = new ServerThread(conn);
		            tt.start();
		        }
			} catch (IOException exception) {
				exception.printStackTrace();
			}
		}
	}

	/**
	 * Class representing DNS and Port pair
	 * 
	 * @author Adib Alwani, Bhavin Vora
	 */
	static class DNSPortPair {
		private final String dns;
		private final int instanceNumber;
		private final boolean host;
		
		public DNSPortPair(String dns, int instanceNumber, boolean host) {
			this.dns = dns;
			this.instanceNumber = instanceNumber;
			this.host = host;
		}

		public String getDns() {
			return dns;
		}

		public int getInstanceNumber() {
			return instanceNumber;
		}

		public boolean isHost() {
			return host;
		}
	}
}
