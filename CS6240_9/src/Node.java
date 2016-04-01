import java.util.List;


/**
 * Main class for running on Node
 * 
 * @author Adib Alwani, Bhavin Vora
 */
public class Node {
	
	private static final String INPUT_DIRECTORY = "input";
	
	public static void main(String args[]) {
		NetworkManager networkManager = new NetworkManager();
		networkManager.spawnServers();
		Reader reader = new Reader();
		System.out.println("Reading Input Files");
		List<TempDetails> tempDetails = reader.readFileOrDirectory(INPUT_DIRECTORY);
		System.out.println("Read Input Files");
		Sampling sampling = new Sampling();
		System.out.println("Sampling Data");
		List<TempDetails> samples = sampling.sampleData(tempDetails);
		System.out.println("Sampled Data of size: " + samples.size());
		networkManager.spawnClients();
		networkManager.fillPartition(samples);
		System.out.println("Sampling Completed");
		System.out.println("Sorting " + tempDetails.size() + " data");
		//networkManager.sortData(tempDetails);
		System.out.println("Sorted data");
		System.exit(0);
	}
}