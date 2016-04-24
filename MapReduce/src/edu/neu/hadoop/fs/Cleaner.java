package edu.neu.hadoop.fs;

import java.io.File;
import java.nio.file.FileSystem;


/**
 * Class used to delete files from the {@link FileSystem}
 * 
 * @author Adib Alwani
 */
public class Cleaner {
	
	/**
	 * Delete the given directory and its sub-directories and files
	 * 
	 * @param file The directory to be deleted
	 */
	public void deleteDirectory(File file) {
		if (file.exists()) {
			File[] files = file.listFiles();

			for (File f : files) {
				if (f.isDirectory()) {
					deleteDirectory(f);
				} else {
					f.delete();
				}
			}
		}

		file.delete();
	}

	/**
	 * Delete the given file
	 * 
	 * @param file The file to be deleted
	 */
	public void deletefile(File file) {
		if (file.exists()) {
			file.delete();
		}
	}
}
