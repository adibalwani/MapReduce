import java.io.IOException;

/**
 * Handle client request
 * 
 * @author Adib Alwani, Bhavin Vora
 */
public class ServerThread extends Thread {
    final TextSocket conn;
    final DismissListener listener;
    
    public interface DismissListener {
    	/**
    	 * Method to call after finishing task
    	 *  
    	 * @param tempDetails instance of TempDetails
    	 */
    	public void onDismiss(TempDetails tempDetails);
    }

    public ServerThread(TextSocket conn, DismissListener listener) {
        this.conn = conn;
        this.listener = listener;
    }

    @Override 
    public void run() {
        try {
            handleConn();
        } catch (IOException exception) {
        	exception.printStackTrace();
        }
    }

    /**
     * Handle client request
     * 
     * @throws IOException
     */
    void handleConn() throws IOException {
        String line = conn.getln();
        TempDetails tempDetails = TempDetails.fromString(line);
        listener.onDismiss(tempDetails);
    }
}


