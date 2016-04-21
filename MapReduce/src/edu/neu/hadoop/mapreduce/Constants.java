package edu.neu.hadoop.mapreduce;

public class Constants {
	
	public static final String PARTITION_FOLDER_NAME = "partition/";
	public static final String MERGED_FILE_NAME = "merged";
	public static final String INPUT_FOLDER_NAME = "input";
	public static final String DNS_FILE_NAME = "instance-dns";
	
	public static final int MASTER_PORT = 9999;
	public static final int MAPPER_PORT = 10000;
	public static final int REDUCER_PORT = 10001;
	
	// Usage: PARTITION_TO_S3_COMMAND_1 + BUCKETNAME + PARTITION_TO_S3_COMMAND_2
	public static final String PARTITION_TO_S3_COMMAND_1 = 
			"aws s3 cp " + PARTITION_FOLDER_NAME + " ";
	public static final String PARTITION_TO_S3_COMMAND_2 = 
			" --recursive";
	
	public static final String S3_TO_PARTITION_COMMAND = "python getS3.py " + PARTITION_FOLDER_NAME;
	
	private Constants() { }
}
