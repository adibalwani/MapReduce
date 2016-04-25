package edu.neu.hadoop.fs;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import edu.neu.hadoop.conf.Configuration;
import edu.neu.hadoop.mapreduce.Constants;
import edu.neu.hadoop.mapreduce.network.HostNameManager;
import edu.neu.hadoop.mapreduce.worker.ReducerThread;

/**
 * An abstract base class for a fairly generic filesystem.  It
 * may be implemented as a distributed filesystem, or as a "local"
 * one that reflects the locally-connected disk.
 *
 * The local implementation is {@link LocalFileSystem} and distributed
 * implementation is DistributedFileSystem.
 * 
 * @author Adib Alwani
 */
public class FileSystem {

	/**
	 * Get a filesystem instance based on the uri and the passed configuration
	 * 
	 * @param uri of the filesystem
	 * @param conf the configuration to use
	 * @throws IOException
	 */
	public static FileSystem get(URI uri, Configuration conf) throws IOException {
		File folder = new File(uri.getPath());
		System.out.println(uri.getPath());
		if (!folder.exists()) {
			folder.mkdirs();
		}
		return new FileSystem();
	}
	
	/**
	 * Get a filesystem instance based on the passed configuration
	 * 
	 * @param conf the configuration to use
	 * @throws IOException
	 */
	public static FileSystem get(Configuration conf) throws IOException {
		return new FileSystem();
	}
	
	/**
	 * Create an {@link FSDataOutputStream} to the indicated {@link Path}
	 * 
	 * @param file the file to create
	 * @return {@link FSDataOutputStream} object
	 * @throws IOException
	 */
	public FSDataOutputStream create(Path file) throws IOException {
		return new FSDataOutputStream(file.getPath());
	}
	
	/**
	 * Opens an FSDataInputStream at the indicated Path.
	 * 
	 * @param file the file to open
	 * @return {@link FSDataInputStream} object
	 * @throws IOException
	 */
	public FSDataInputStream open(Path file) throws IOException {
		HostNameManager hostNameManager = new HostNameManager();
		boolean clusterMode = hostNameManager.getWorkerNodes().size() != 0;
		if (clusterMode) {
			try {
				Runtime runtime = Runtime.getRuntime();
				if (file.getFileName() != null) {
					Process process = runtime.exec(Constants.s3ToLocal(
							file.getPath(), Constants.OUTPUT_FOLDER_NAME));
					process.waitFor();
					return new FSDataInputStream(Constants.OUTPUT_FOLDER_NAME
							+ "/" + file.getFileName());
				} else {
					String bucketName = 
							ReducerThread.configuration.getOutputPath().getBucketPath();
					Process process = runtime.exec(Constants.s3ToLocal(
							bucketName + file.getPath(), "."));
					process.waitFor();
					return new FSDataInputStream(file.getPath());
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
		}
		
		if (file.getFileName() == null) {
			return new FSDataInputStream(file.getPath());
		} else {
			return new FSDataInputStream(file.getPath() + "/" + file.getFileName());
		}
	}
	
	/**
	 * Check whether file exists or not for the provided {@link Path}
	 */
	public boolean exists(Path path) {
		boolean clusterMode = path.getPath().contains("s3");
		if (clusterMode) {
			return true;
		}
		File file = new File(path.getPath());
		return file.exists();
	}
}
