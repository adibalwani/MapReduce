import java.util.List;


/**
 * Main class for running on Node
 * 
 * @author Adib Alwani, Bhavin Vora
 */
public class Node {
	
	private static final String INPUT_DIRECTORY = "input";
	
	public static void main(String args[]) {
		NetworkManager networkManager = NetworkManager.newInstance();
		networkManager.spawnServers();
		Reader reader = new Reader();
		List<TempDetails> tempDetails = reader.readFileOrDirectory(INPUT_DIRECTORY);
		Sampling sampling = new Sampling();
		List<TempDetails> samples = sampling.sampleData(tempDetails);
		networkManager.spawnClients();
		networkManager.broadcastSamples(samples);
		networkManager.closeConnections();
	}
}