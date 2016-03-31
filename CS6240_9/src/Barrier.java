import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Class used for handling barriers
 * 
 * @author Adib Alwani, Bhavin Vora
 */
public class Barrier {

	private static Object lock;
	private static int barrierCount;

	public Barrier(int port) {
		lock = new Object();
		barrierCount = 0;
		new ListenThread(port).start();
	}

	/**
	 * Increment the counter
	 */
	static void inc() {
		synchronized (lock) {
			barrierCount++;
			lock.notifyAll();
		}
	}

	/**
	 * Start the barrier
	 * 
	 * @param nodes Number of nodes to wait for
	 */
	void barrier(int nodes) {
		inc(); // This node is in the barrier.

		synchronized (lock) {
			while (barrierCount < nodes) {
				try {
					lock.wait();
				} catch (InterruptedException ee) {
					// Do Nothing
				}
			}
		}
	}
	
	/**
	 * Thread used for updating barrier completion
	 * 
	 * @author Adib Alwani, Bhavin Vora
	 */
	static class CompleteThread extends Thread {
		final String hostName;
		final int port;

		public CompleteThread(String hostName, int port) {
			this.hostName = hostName;
			this.port = port;
		}

		@Override
		public void run() {
			while (true) {
				try {
					Socket ss = new Socket(hostName, port);
					ss.close();
					break;
				} catch (IOException e) {
					System.out.println("Barrier has not yet been created for " + hostName);
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
	 * Thread used for listening barrier changes
	 * 
	 * @author Adib Alwani, Bhavin Vora
	 */
	static class ListenThread extends Thread {
		final int port;

		public ListenThread(int port) {
			this.port = port;
		}

		@SuppressWarnings("resource")
		@Override
		public void run() {
			try {
				ServerSocket svr = new ServerSocket(port);
				Socket conn;
				
				while (null != (conn = svr.accept())) {
					new ConnThread(conn).start();
				}
			} catch (Exception ee) {
				System.err.println("Error in ListenThread: " + ee.toString());
			}
		}
	}

	/**
	 * Thread used for handling barrier change
	 * 
	 * @author Adib Alwani, Bhavin Vora
	 */
	static class ConnThread extends Thread {
		final Socket conn;

		public ConnThread(Socket conn) {
			this.conn = conn;
		}

		@Override
		public void run() {
			try {
				Barrier.inc();
				conn.close();
			} catch (Exception ee) {
				System.err.println("IO Error: " + ee.toString());
			}
		}
	}

}
