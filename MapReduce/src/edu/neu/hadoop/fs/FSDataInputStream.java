package edu.neu.hadoop.fs;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

/**
 * Utility that wraps a {@link InputStream} in a {@link DataInputStream}
 * 
 * @author Adib Alwani
 */
public class FSDataInputStream extends FileInputStream {

	public FSDataInputStream(String name) throws FileNotFoundException {
		super(name);
	}	
}
