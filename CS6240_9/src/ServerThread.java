import java.io.IOException;

public class ServerThread extends Thread {
    final TextSocket conn;

    public ServerThread(TextSocket conn) {
        this.conn = conn;
    }

    @Override 
    public void run() {
        try {
            handleConn();
        }
        catch (IOException ee) {
            System.err.println("IO Error: " + ee.toString());
        }
    }

    void handleConn() throws IOException {
        String req = conn.getln();
        while (!conn.getln().equals("")) {
            // skip it.
        }
        
        String[] parts = req.split(" ");
        if (!parts[0].equals("GET")) {
            throw new IOException("Expected GET");
        }
        if (!parts[2].equals("HTTP/1.1")) {
            throw new IOException("Expected HTTP/1.1");
        }

        String path = parts[1];

        conn.putln("HTTP/1.1 200 OK");
        conn.putln("Content-type: text/plain");
        conn.putln("");
        conn.putln("Hello, path = " + path);
        conn.close();

        System.out.println("Handled GET " + path + " in thread " + Thread.currentThread().getId());
    }
}


