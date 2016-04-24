package edu.neu.hadoop.fs;

import java.io.IOException;
import java.io.Serializable;

import edu.neu.hadoop.conf.Configuration;

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
	
	public Path(String path, String fileName) {
		this.path = path + "/" + fileName;
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
	
	/**
	 * Return the {@link FileSystem} that owns this Path
	 */
	public FileSystem getFileSystem(Configuration conf) throws IOException {
		return FileSystem.get(null, conf);
	}

	public String getPath() {
		return path;
	}
}
