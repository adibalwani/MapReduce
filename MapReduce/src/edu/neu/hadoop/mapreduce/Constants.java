package edu.neu.hadoop.mapreduce;

public class Constants {
	
	public static final String PARTITION_FOLDER_NAME = "partition/";
	public static final String MERGED_FILE_NAME = "merged";
	public static final String DNS_FILE_NAME = "instance-dns";
	
	public static final int MASTER_PORT = 9999;
	public static final int MAPPER_PORT = 10000;
	public static final int REDUCER_PORT = 10001;

	private Constants() { }
}
