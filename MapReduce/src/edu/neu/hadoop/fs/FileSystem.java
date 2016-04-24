package edu.neu.hadoop.fs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URI;

import edu.neu.hadoop.conf.Configuration;

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
		return (FSDataOutputStream) 
				new ObjectOutputStream(new FileOutputStream(file.getPath()));
	}
	
	/**
	 * Opens an FSDataInputStream at the indicated Path.
	 * 
	 * @param file the file to open
	 * @return {@link FSDataInputStream} object
	 * @throws IOException
	 */
	public FSDataInputStream open(Path file) throws IOException {
		return new FSDataInputStream(file.getPath());
	}
	
	/**
	 * Check whether file exists or not for the provided {@link Path}
	 */
	public boolean exists(Path path) {
		File file = new File(path.getPath());
		return file.exists();
	}
}
