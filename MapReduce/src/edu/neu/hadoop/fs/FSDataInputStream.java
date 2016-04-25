package edu.neu.hadoop.fs;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

/**
 * Utility that wraps a {@link InputStream} in a {@link FileInputStream}
 * 
 * @author Adib Alwani
 */
public class FSDataInputStream extends FileInputStream {

	public FSDataInputStream(String path) throws FileNotFoundException {
		super(path);
	}
	
	@Override
	public void close() throws IOException {
		super.close();
	}
}
