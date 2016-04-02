import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Thread to spawn a server for the given client DNSPort pair
 * 
 * @author Adib Alwani
 */
public class Server extends Thread {

	final TextSocket.Server svr;
	final List<TempDetails> listDetails;
	boolean dataRead;

	public Server(TextSocket.Server svr) {
		this.svr = svr;
		listDetails = new ArrayList<TempDetails>();
		dataRead = false;
	}
	
	@Override
	public void run() {
		try {
			TextSocket conn;
			while (null != (conn = svr.accept())) {
				for (String line : conn) {
					if (line != null) {
						if (line.equals("done")) {
							dataRead = true;
						} else {
							TempDetails tempDetails = TempDetails.fromString(line);
							listDetails.add(tempDetails);
						}
					}
				}
			}
		} catch (IOException exception) {
			exception.printStackTrace();
		}
	}
	
	public void setDataRead(boolean dataRead) {
		this.dataRead = dataRead;
	}

	public boolean isDataRead() {
		return dataRead;
	}

	public List<TempDetails> getListDetails() {
		return listDetails;
	}
}