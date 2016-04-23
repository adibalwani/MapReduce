package edu.neu.hadoop.mapreduce;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;

import edu.neu.hadoop.conf.Configuration;
import edu.neu.hadoop.io.Writable;
import edu.neu.hadoop.mapreduce.lib.input.FileIterator;
import edu.neu.hadoop.mapreduce.network.HostNameManager;

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
		HostNameManager hostNameManager = new HostNameManager();
		this.conf = conf;
		this.iterator = new FileIterator(hostNameManager.getOwnInstanceNumber(), 
				Constants.MERGED_FILE_NAME, conf);
		boolean clusterMode = conf.getOutputPath().getPath().contains("s3");
		String folderUri;
		if (clusterMode) {
			folderUri = Constants.OUTPUT_FOLDER_NAME + "/";
		} else {
			folderUri = conf.getOutputPath().getPath() + "/";
		}
		File folder = new File(folderUri);
		if (!folder.exists()) {
			folder.mkdirs();
		}
		String uri = folderUri + Constants.REDUCER_OUTPUT_FORMAT +
				hostNameManager.getOwnInstanceNumber();
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
