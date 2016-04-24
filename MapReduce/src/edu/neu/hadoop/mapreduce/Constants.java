package edu.neu.hadoop.mapreduce;

import java.nio.file.FileSystem;

/**
 * Constants defined in the Map-Reduce framework
 * 
 * @author Adib Alwani
 */
public class Constants {
	
	public static final String PARTITION_FOLDER_NAME = "partition/";
	public static final String MERGED_FILE_NAME = "merged";
	public static final String INPUT_FOLDER_NAME = "input";
	public static final String OUTPUT_FOLDER_NAME = "output";
	public static final String REDUCER_OUTPUT_FORMAT = "part-r-0000";
	public static final String DNS_FILE_NAME = "instance-dns";
	
	public static final int MASTER_PORT = 9999;
	public static final int MAPPER_PORT = 10000;
	public static final int REDUCER_PORT = 10001;
	
	/**
	 * AWS command for transferring data from S3 to partitions in {@link FileSystem}
	 * 
	 * @param bucketPath Bucket Name
	 * @param instanceNumber Own instance Number
	 * @return Command
	 */
	public static String s3ToPartition(String bucketPath, int instanceNumber) {
		StringBuilder builder = new StringBuilder();
		builder.append("aws s3 cp ");
		builder.append(bucketPath);
		builder.append(PARTITION_FOLDER_NAME);
		builder.append(instanceNumber);
		builder.append(' ');
		builder.append(PARTITION_FOLDER_NAME);
		builder.append(instanceNumber);
		builder.append(" --recursive");
		return builder.toString();
	}
	
	/**
	 * AWS command for transferring data from partitions in {@link FileSystem} to S3
	 * 
	 * @param bucketPath Bucket Name
	 * @return Command
	 */
	public static String partitionToS3(String bucketPath) {
		StringBuilder builder = new StringBuilder();
		builder.append("aws s3 cp ");
		builder.append(PARTITION_FOLDER_NAME);
		builder.append(' ');
		builder.append(bucketPath);
		builder.append(PARTITION_FOLDER_NAME);
		builder.append(" --recursive");
		return builder.toString();
	}
	
	/**
	 * AWS command for transferring data from output in {@link FileSystem} to S3
	 * 
	 * @param outputFolder S3 output path
	 * @return Command
	 */
	public static String outputToS3(String outputFolder) {
		StringBuilder builder = new StringBuilder();
		builder.append("aws s3 cp ");
		builder.append(OUTPUT_FOLDER_NAME);
		builder.append(' ');
		builder.append(outputFolder);
		builder.append(" --recursive");
		return builder.toString();
	}
	
	private Constants() { }
}
