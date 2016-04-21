package edu.neu.hadoop.mapreduce.network;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Client class to handle Object I/O via sockets
 * 
 * @author Adib Alwani
 */
public class ObjectSocket {
	final Socket socket;
	final ObjectOutputStream clientOutputStream;
	final ObjectInputStream clientInputStream;

	public ObjectSocket(String host, int port) throws IOException {
		this(new Socket(host, port));
	}

	public ObjectSocket(Socket socket) throws IOException {
		this.socket = socket;
		try {
			clientOutputStream = new ObjectOutputStream(socket.getOutputStream());
			clientInputStream = new ObjectInputStream(socket.getInputStream());
		} catch (UnsupportedEncodingException ee) {
			throw new IOException(ee.getMessage());
		}
	}

	/**
	 * Write the given object in the {@link Socket} stream
	 * 
	 * @param object Object to write
	 * @throws IOException
	 */
	public void write(Object object) throws IOException {
		clientOutputStream.writeObject(object);
	}
	
	/**
	 * Return an object read from the {@link Socket} stream
	 * 
	 * @throws ClassNotFoundException
	 * @throws IOException
	 */
	public Object read() throws ClassNotFoundException, IOException {
		return clientInputStream.readObject();
	}
	
	/**
	 * Close the connection
	 * 
	 * @throws IOException
	 */
	public void close() throws IOException {
		socket.close();
	}

	/**
	 * Server class to handle Object I/O via sockets
	 * 
	 * @author Adib Alwani
	 */
	public static class Server {
		final ServerSocket server;

		public Server(int port) throws IOException {
			server = new ServerSocket(port);
		}

		public ObjectSocket accept() throws IOException {
			Socket sock = server.accept();
			if (sock == null) {
				return null;
			} else {
				return new ObjectSocket(sock);
			}
		}
	}
}
