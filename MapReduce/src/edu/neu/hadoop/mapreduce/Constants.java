package edu.neu.hadoop.mapreduce;

/**
 * Constants defined in the Map-Reduce framework
 * 
 * @author Adib Alwani
 */
public class Constants {
	
	public static final String PARTITION_FOLDER_NAME = "partition/";
	public static final String MERGED_FILE_NAME = "merged";
	public static final String INPUT_FOLDER_NAME = "input";
	public static final String DNS_FILE_NAME = "instance-dns";
	
	public static final int MASTER_PORT = 9999;
	public static final int MAPPER_PORT = 10000;
	public static final int REDUCER_PORT = 10001;
	
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
	
	public static String outputToS3(String bucketPath, String outputFolder) {
		StringBuilder builder = new StringBuilder();
		builder.append("aws s3 cp ");
		builder.append(outputFolder);
		builder.append(' ');
		builder.append(bucketPath);
		builder.append(outputFolder);
		builder.append(" --recursive");
		return builder.toString();
	}
	
	private Constants() { }
}
