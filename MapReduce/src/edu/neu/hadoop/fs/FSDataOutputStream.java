package edu.neu.hadoop.fs;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

import edu.neu.hadoop.mapreduce.network.HostNameManager;

/**
 * Utility that wraps a {@link OutputStream} in a {@link DataOutputStream}
 * 
 * @author Adib Alwani
 */
public class FSDataOutputStream extends ObjectOutputStream {
	
	private final Path path;
	private final ObjectOutputStream objectOutputStream;
	
	public FSDataOutputStream(Path path) throws IOException {
		this.path = path;
		this.objectOutputStream = new ObjectOutputStream(
				new FileOutputStream(path.getPath()));
	}
	
	@Override
	public void close() throws IOException {
		super.close();
		HostNameManager hostNameManager = new HostNameManager();
		// Check if in S3 or not
		if (!hostNameManager.getWorkerNodes().isEmpty()) {
			
		}
	}

	public ObjectOutputStream getObjectOutputStream() {
		return objectOutputStream;
	}
}
