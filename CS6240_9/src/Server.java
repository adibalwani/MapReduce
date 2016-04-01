import java.io.IOException;
import java.util.List;

/**
 * Thread to spawn a server for the given client DNSPort pair
 * 
 * @author Adib Alwani
 */
public class Server extends Thread {

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
				for (String line : conn) {
					if (line != null) {
						TempDetails tempDetails = TempDetails.fromString(line);
						listDetails.add(tempDetails);
					}
				}
			}
		} catch (IOException exception) {
			exception.printStackTrace();
		}
	}
}