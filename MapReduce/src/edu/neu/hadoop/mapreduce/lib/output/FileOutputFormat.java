package edu.neu.hadoop.mapreduce.lib.output;

import java.nio.file.FileSystem;

import edu.neu.hadoop.conf.Configuration;
import edu.neu.hadoop.fs.Path;
import edu.neu.hadoop.mapreduce.Job;

/**
 * A base class for {@link OutputFormat}s that read from {@link FileSystem}s.
 * 
 * @author Adib Alwani
 */
public class FileOutputFormat {
	
	/**
	 * Set the {@link Path} of the output directory for the map-reduce job.
	 * 
	 * @param job The job to modify
	 * @param outputDir the {@link Path} of the output directory for the map-reduce job.
	 */
	public static void setOutputPath(Job job, Path outputDir) {
		Configuration conf = job.getConf();
		conf.setOutputPath(outputDir);
	}
}
