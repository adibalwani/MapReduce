import java.io.IOException;

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