package edu.neu.hadoop.mapreduce;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import edu.neu.hadoop.conf.Configuration;
import edu.neu.hadoop.io.Writable;

/**
 * Merger is an utility class used by the Reduce tasks for merging the 
 * partitioned files into a single large file.
 * Users of this class typically call {@link Merger#merge(int, String)} to
 * merger into a single file
 * 
 * @author Adib Alwani
 */
public class Merger {
	
	private Configuration conf;
	
	@SuppressWarnings("unused")
	private Merger() { }
	
	public Merger(Configuration conf) {
		this.conf = conf;
	}
	
	/**
	 * Merge files in the provided partition into one big file
	 * 
	 * @param numPartition Partition number
	 * @param fileName Merged file name
	 */
	public void merge(int numPartition, String fileName) {
		String folderUri = Constants.PARTITION_FOLDER_NAME + String.valueOf(numPartition) + "/";
		File[] files = new File(folderUri).listFiles();
		Class<?> mapOutputKeyClass = conf.getMapOutputKeyClass();
		Class<?> mapOutputValueClass = conf.getMapOutputValueClass();
		ObjectInputStream[] inputStream = new ObjectInputStream[files.length];
		Writable[] key = new Writable[files.length];
		Writable[] value = new Writable[files.length];
		
		try (
			ObjectOutputStream outputStream = 
				new ObjectOutputStream(new FileOutputStream(new File(fileName)));
		) {
			for (int i = 0; i < files.length; i++) {
				inputStream[i] = new ObjectInputStream(new FileInputStream(files[i]));
			}
			
			/*while (hasMoreData) {
				
			}
			
			Writable key = (Writable) mapOutputKeyClass.newInstance();
			Writable value = (Writable) mapOutputValueClass.newInstance();
			key.readFields(inputStream);
			value.readFields(inputStream);*/
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				for (int i = 0; i < files.length; i++) {
					inputStream[i].close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
