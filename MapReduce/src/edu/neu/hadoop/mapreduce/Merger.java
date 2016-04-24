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
	 * @return true iff merger was successful. False, otherwise
	 */
	public boolean merge(int numPartition, String fileName) {
		String folderUri = Constants.PARTITION_FOLDER_NAME + String.valueOf(numPartition) + "/";
		String uri = folderUri + fileName;
		File[] files = new File(folderUri).listFiles();
		if (files.length == 0) {
			return false;
		}
		
		Class<?> mapOutputKeyClass = conf.getMapOutputKeyClass();
		Class<?> mapOutputValueClass = conf.getMapOutputValueClass();
		ObjectInputStream[] inputStream = new ObjectInputStream[files.length];
		Writable[] key = new Writable[files.length];
		Writable[] value = new Writable[files.length];
		int filesRead = 0;
		
		try (
			ObjectOutputStream outputStream = 
				new ObjectOutputStream(new FileOutputStream(new File(uri)));
		) {
			for (int i = 0; i < files.length; i++) {
				inputStream[i] = new ObjectInputStream(new FileInputStream(files[i]));
				key[i] = (Writable) mapOutputKeyClass.newInstance();
				value[i] = (Writable) mapOutputValueClass.newInstance();
				key[i].readFields(inputStream[i]);
				value[i].readFields(inputStream[i]);
			}
			
			while (filesRead != files.length) {
				int min = min(key);
				key[min].write(outputStream);
				value[min].write(outputStream);
				if (inputStream[min].available() != 0) {
					key[min].readFields(inputStream[min]);
					value[min].readFields(inputStream[min]);
				} else {
					key[min] = null;
					value[min] = null;
					filesRead++;
				}
			}
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
		
		return true;
	}
	
	@SuppressWarnings("unchecked")
	private int min(Writable[] key) {
		int min = 0;
		
		for (int i = 1; i < key.length; i++) {
			if (key[i] == null) {
				continue;
			}
			if ((key[min] == null) ||
					((Comparable<Writable>) key[i]).compareTo(key[min]) < 0) {
				min = i;
			}
		}
		
		return min;
	}
}
