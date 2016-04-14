package edu.neu.hadoop.mapreduce;

import edu.neu.hadoop.fs.Path;
import edu.neu.hadoop.mapreduce.lib.input.FileIterator;

/**
 * The <code>Context</code> passed on to the {@link Mapper} 
 * or {@link Reducer} implementations.
 * 
 * @author Adib Alwani
 */
public abstract class Context {
	
	protected Path[] inputPaths;
	protected FileIterator iterator;
	
	/**
	 * Data emitted by either {@link Mapper} or {@link Reducer}
	 */
	public <KEYOUT, VALUEOUT> void write(KEYOUT key, VALUEOUT value) { }

	public Path[] getInputPaths() {
		return inputPaths;
	}
	
	public FileIterator getFileIterator() {
		return iterator;
	}
}
