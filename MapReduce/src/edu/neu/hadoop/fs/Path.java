package edu.neu.hadoop.fs;

/**
 * Names a file or directory in a {@link FileSystem}.
 * 
 * @author Adib Alwani
 */
public class Path {
	
	private String path;
	
	public Path(String path) {
		this.path = path;
	}

	public String getPath() {
		return path;
	}
}
