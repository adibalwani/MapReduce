package edu.neu.hadoop.mapreduce.lib.input;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.nio.file.FileSystem;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import edu.neu.hadoop.conf.Configuration;
import edu.neu.hadoop.io.Writable;
import edu.neu.hadoop.mapreduce.KeyValue;

/**
 * Class to handle read operations from {@link FileSystem}
 * 
 * @author Adib Alwani
 */
public class Reader {
	
	private static final String FOLDER_NAME = "partition/";
	
	public interface ReadListener {
		/**
		 * Method to call on reading a single line
		 *  
		 * @param line Line read
		 */
		public void onReadLine(String line);
	}

	/**
	 * Read from the given file or directory.
	 * After each line read, call {@link ReadListener#onReadLine(String)}
	 * 
	 * @param fileName The name of a file or a folder
	 */
	public void readFileOrDirectory(String fileName, ReadListener listener) {

		// gets the list of files in a folder (if user has submitted
		// the name of a folder) or gets a single file name (is user
		// has submitted only the file name)
		List<File> files = new ArrayList<File>();
		addFiles(new File(fileName), files);

		for (File file : files) {
			if (file.getName().contains(".tar")) {
				readGZIPFile(file, listener);
			} else {
				readFile(file, listener);
			}
		}
	}
	
	/**
	 * Read from the given compressed file
	 * After each line read, call {@link ReadListener#onReadLine(String)}
	 * 
	 * @param file The name of a file
	 */
	public void readGZIPFile(File file, ReadListener listener) {
		try (
			InputStream gzipStream = new GZIPInputStream(new FileInputStream(file));
			BufferedReader reader = new BufferedReader(
				new InputStreamReader(gzipStream));
		) {
			String line;
			while ((line = reader.readLine()) != null) {
				listener.onReadLine(line);
			}
		} catch (Exception exception) {
			exception.printStackTrace();
		}
	}
	
	/**
	 * Read from the given file
	 * After each line read, call {@link ReadListener#onReadLine(String)}
	 * 
	 * @param file The name of a file
	 */
	public void readFile(File file, ReadListener listener) {
		try (
			FileReader fileReader = new FileReader(file);
			BufferedReader reader = new BufferedReader(fileReader);
		) {
			String line;
			while ((line = reader.readLine()) != null) {
				listener.onReadLine(line);
			}
		} catch (Exception exception) {
			exception.printStackTrace();
		}
	}
	
	/**
	 * Add the file to the given List of files. If the file is a directory
	 * search its files and sub-directories
	 * 
	 * @param file The file or directory to add from
	 * @param files The list of files to add to
	 */
	private void addFiles(File file, List<File> files) {
		// File or Directory doesn't exist
		if (!file.exists()) {
			System.out.println(file + " does not exist.");
			return;
		}

		// Provided a directory
		if (file.isDirectory()) {
			for (File f : file.listFiles()) {
				addFiles(f, files);
			}
		} else {
			files.add(file);
		}
	}
	
	/**
	 * Read the {@link KeyValue} pairs from the {@link FileSystem} in the
	 * following structure: ./partition/numPartition/fileName 
	 * 
	 * @param numPartition Partition number
	 * @param fileName Name of the file
	 * @param conf Configuration object
	 */
	public void read(int numPartition, String fileName, Configuration conf) {
		String folderUri = FOLDER_NAME + String.valueOf(numPartition) + "/";
		String uri = folderUri + fileName;
		File file = new File(uri);
		Class<?> mapOutputKeyClass = conf.getMapOutputKeyClass();
		Class<?> mapOutputValueClass = conf.getMapOutputValueClass();
		
		try (
			ObjectInputStream inputStream = 
				new ObjectInputStream(new FileInputStream(file));
		) {
			while (inputStream.available() != 0) {
				Writable key = (Writable) mapOutputKeyClass.newInstance();
				Writable value = (Writable) mapOutputValueClass.newInstance();
				key.readFields(inputStream);
				value.readFields(inputStream);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
