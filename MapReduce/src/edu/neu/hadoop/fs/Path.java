package edu.neu.hadoop.fs;

import java.io.Serializable;
import java.nio.file.FileSystem;

/**
 * Names a file or directory in a {@link FileSystem}.
 * 
 * @author Adib Alwani
 */
@SuppressWarnings("serial")
public class Path implements Serializable {
	
	private String path;
	
	public Path(String path) {
		this.path = path;
	}

	public String getPath() {
		return path;
	}
}
