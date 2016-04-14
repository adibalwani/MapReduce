package edu.neu.hadoop.mapreduce;

import java.io.IOException;

import edu.neu.hadoop.mapreduce.lib.input.FileIterator;


/**
 * Reduces a set of intermediate values which share a key to a smaller set of values.
 * 
 * @author Adib Alwani
 */
public class Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

	/**
	 * Called once at the start of the task.
	 */
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// NOTHING
	}

	/**
	 * This method is called once for each key. Most applications will define
	 * their reduce class by overriding this method. The default implementation
	 * is an identity function.
	 */
	@SuppressWarnings("unchecked")
	protected void reduce(KEYIN key, Iterable<VALUEIN> values, Context context)
			throws IOException, InterruptedException {
		for (VALUEIN value : values) {
			context.write((KEYOUT) key, (VALUEOUT) value);
		}
	}

	/**
	 * Called once at the end of the task.
	 */
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		// NOTHING
	}

	/**
	 * Advanced application writers can use the
	 * {@link #run(org.apache.hadoop.mapreduce.Reducer.Context)} method to
	 * control how the reduce task works.
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 */
	@SuppressWarnings("unchecked")
	public void run(Context context) throws IOException, InterruptedException,
			InstantiationException, IllegalAccessException {
		setup(context);
		FileIterator iterator = context.getFileIterator();
		while (iterator.hasNext()) {
			Iterable<VALUEIN> values = (Iterable<VALUEIN>) iterator.next();
			reduce((KEYIN) iterator.getKey(), values, context);
		}
		cleanup(context);
	}
}
