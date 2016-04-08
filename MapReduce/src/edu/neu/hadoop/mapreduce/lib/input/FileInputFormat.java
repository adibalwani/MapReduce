package edu.neu.hadoop.mapreduce.lib.input;

import java.io.IOException;

import edu.neu.hadoop.conf.Configuration;
import edu.neu.hadoop.fs.Path;
import edu.neu.hadoop.mapreduce.Job;

/**
 * <code>FileInputFormat</code> is the base class for all file-based 
 * <code>InputFormat</code>s.
 * 
 * @author Adib Alwani
 */
public class FileInputFormat {
	
	/**
	 * Add a {@link Path} to the list of inputs for the map-reduce job.
	 * 
	 * @param job The {@link Job} to modify
	 * @param path {@link Path} to be added to the list of inputs for the
	 *            map-reduce job.
	 */
	public static void addInputPath(Job job, Path path) throws IOException {
		Configuration conf = job.getConfiguration();
		conf.setInputPath(new Path[]{ path });
	}
}
