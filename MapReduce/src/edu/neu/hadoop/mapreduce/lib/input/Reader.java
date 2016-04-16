package edu.neu.hadoop.mapreduce.lib.input;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.FileSystem;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * Class to handle read operations from {@link FileSystem}
 * 
 * @author Adib Alwani
 */
public class Reader {
	
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
			if (file.getName().contains(".gz")) {
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
}
