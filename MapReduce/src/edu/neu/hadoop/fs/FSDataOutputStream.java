package edu.neu.hadoop.fs;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import edu.neu.hadoop.mapreduce.Constants;
import edu.neu.hadoop.mapreduce.worker.ReducerThread;

/**
 * Utility that wraps a {@link OutputStream} in a {@link FileOutputStream}
 * 
 * @author Adib Alwani
 */
public class FSDataOutputStream extends FileOutputStream {
	
	private String path;
	
	public FSDataOutputStream(String path) throws FileNotFoundException {
		super(path);
		this.path = path;
	}

	@Override
	public void close() throws IOException {
		super.close();
		boolean clusterMode = ReducerThread.configuration != null;
		if (clusterMode) {
			try {
				Runtime runtime = Runtime.getRuntime();
				String bucketName = 
						ReducerThread.configuration.getOutputPath().getBucketPath();
				Process process = runtime.exec(Constants.LocalToS3(path, 
						bucketName + Constants.MODEL_FOLDER_NAME));
				process.waitFor();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
