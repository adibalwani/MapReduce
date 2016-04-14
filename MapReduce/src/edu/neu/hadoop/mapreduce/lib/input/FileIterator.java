package edu.neu.hadoop.mapreduce.lib.input;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.file.FileSystem;
import java.util.ArrayList;
import java.util.List;

import edu.neu.hadoop.conf.Configuration;
import edu.neu.hadoop.io.Writable;
import edu.neu.hadoop.mapreduce.Constants;
import edu.neu.hadoop.mapreduce.KeyValue;

/**
 * 
 * 
 * @author Adib Alwani
 */
@SuppressWarnings("unused")
public class FileIterator {
	
	private Configuration conf;
	private Writable key;
	private Writable nextKey;
	private Writable nextValue;
	private Class<?> mapOutputKeyClass;
	private Class<?> mapOutputValueClass;
	private ObjectInputStream inputStream;
	private List<Writable> valuesList;
	
	/**
	 * Instantiate an instance of {@link FileIterator}.
	 * Read the {@link KeyValue} pairs from the {@link FileSystem} in the
	 * following structure: ./partition/numPartition/fileName 
	 * 
	 * @param numPartition Partition number
	 * @param fileName Name of the file
	 * @param conf {@link Configuration} object
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	public FileIterator(int numPartition, String fileName, Configuration conf)
			throws FileNotFoundException, IOException, InstantiationException,
			IllegalAccessException {
		this.conf = conf;
		mapOutputKeyClass = conf.getMapOutputKeyClass();
		mapOutputValueClass = conf.getMapOutputValueClass();
		String folderUri = Constants.PARTITION_FOLDER_NAME + String.valueOf(numPartition) + "/";
		String uri = folderUri + fileName;
		inputStream = new ObjectInputStream(new FileInputStream(new File(uri)));
		valuesList = new ArrayList<Writable>();
		if (inputStream.available() != 0) {
			Writable key = (Writable) mapOutputKeyClass.newInstance();
			key.readFields(inputStream);
			Writable value = (Writable) mapOutputValueClass.newInstance();
			value.readFields(inputStream);
			this.nextKey = key;
			this.nextValue = value;
		}
	}
	
	/**
	 * Check whether input data is available to be read
	 * 
	 * @return true iff there is more input. False, otherwise
	 * @throws IOException
	 */
	public boolean hasNext() throws IOException {
		return !nextKey.equals(key);
	}
	
	/**
	 * Return an {@link Iterable} to the next set of values for a key
	 * 
	 * @return {@link Iterable} to the values
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws IOException
	 */
	public Iterable<Writable> next() throws InstantiationException,
			IllegalAccessException, IOException {
		valuesList.clear();
		
		this.key = this.nextKey;
		valuesList.add(this.nextValue);
		
		while (inputStream.available() != 0) {
			Writable key = (Writable) mapOutputKeyClass.newInstance();
			key.readFields(inputStream);
			Writable value = (Writable) mapOutputValueClass.newInstance();
			value.readFields(inputStream);
			if (!key.equals(this.key)) {
				this.nextKey = key;
				this.nextValue = value;
				break;
			}
			valuesList.add(value);
		}
		
		return valuesList;
	}

	public Writable getKey() {
		return key;
	}
	
}
