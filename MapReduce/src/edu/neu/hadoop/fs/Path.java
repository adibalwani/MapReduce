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
	
	/**
	 * Get the bucket name from String (if exists)
	 * <p> Caller of this method must ensure a valid path is provided </p>
	 * 
	 * @return Bucket Path
	 */
	public String getBucketPath() {
        String[] first = path.split("//");
        StringBuilder builder = new StringBuilder();
        builder.append(first[0]);
        builder.append("//");
        String[] second = first[1].split("/");
        builder.append(second[0]);
        builder.append('/');
        return builder.toString();
	}

	public String getPath() {
		return path;
	}
}
