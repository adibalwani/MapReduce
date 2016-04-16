package edu.neu.hadoop.mapreduce;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;

import edu.neu.hadoop.conf.Configuration;
import edu.neu.hadoop.io.Writable;
import edu.neu.hadoop.mapreduce.lib.input.FileIterator;

/**
 * The <code>Context</code> passed on to the {@link Reducer} implementation
 * 
 * @author Adib Alwani
 */
@SuppressWarnings("unused")
public class ReduceContext extends Context {
	
	private Configuration conf;
	private PrintWriter outputStream;

	public ReduceContext(Configuration conf) throws FileNotFoundException,
			InstantiationException, IllegalAccessException, IOException {
		this.conf = conf;
		this.iterator = new FileIterator(0, Constants.MERGED_FILE_NAME, conf);
		String folderUri = conf.getOutputPath().getPath() + "/";
		File folder = new File(folderUri);
		if (!folder.exists()) {
			folder.mkdirs();
		}
		String uri = folderUri + Thread.currentThread().getName();
		this.outputStream = new PrintWriter(new File(uri));
	}
	
	@Override
	public <KEYOUT, VALUEOUT> void write(KEYOUT key, VALUEOUT value) {
		outputStream.println(key.toString() + "\t" + value.toString());
	}
	
	/**
	 * Close the print stream
	 */
	public void close() {
		outputStream.close();
	}
	
	public FileIterator getIterator() {
		return iterator;
	}
}
