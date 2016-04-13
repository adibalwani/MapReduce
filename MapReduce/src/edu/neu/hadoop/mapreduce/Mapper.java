package edu.neu.hadoop.mapreduce;

import java.io.IOException;

import edu.neu.hadoop.fs.Path;
import edu.neu.hadoop.io.Text;
import edu.neu.hadoop.mapreduce.lib.input.Reader;

/**
 * Maps input key/value pairs to a set of intermediate key/value pairs.  
 * 
 * <p>Maps are the individual tasks which transform input records into a 
 * intermediate records. The transformed intermediate records need not be of 
 * the same type as the input records. A given input pair may map to zero or 
 * many output pairs.</p>
 * 
 * <p>The framework first calls 
 * {@link #setup(edu.neu.hadoop.mapreduce.Mapper.Context)}, followed by
 * {@link #map(Object, Object, edu.neu.hadoop.mapreduce.Mapper.Context)}
 * for each key/value pair in the <code>InputSplit</code>. Finally 
 * {@link #cleanup(edu.neu.hadoop.mapreduce.Mapper.Context)} is called.</p>
 * 
 * @author Adib Alwani
 */
public class Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

	/**
	 * Called once at the beginning of the task.
	 */
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// NOTHING
	}

	/**
	 * Called once for each key/value pair in the input split. Most applications
	 * should override this, but the default is the identity function.
	 */
	@SuppressWarnings("unchecked")
	protected void map(KEYIN key, VALUEIN value, Context context)
			throws IOException, InterruptedException {
		context.write((KEYOUT) key, (VALUEOUT) value);
	}

	/**
	 * Called once at the end of the task.
	 */
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		// NOTHING
	}

	/**
	 * Expert users can override this method for more complete control over the
	 * execution of the Mapper.
	 * 
	 * @param context
	 * @throws IOException
	 */
	public void run(final Context context) throws IOException, InterruptedException {
		setup(context);
		Reader reader = new Reader();
		for (Path path : context.getInputPaths()) {
			reader.readFileOrDirectory(path.getPath(), new Reader.ReadListener() {
				@SuppressWarnings("unchecked")
				@Override
				public void onReadLine(String line) {
					try {
						map(null, (VALUEIN) new Text(line), context);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});
		}
		cleanup(context);
	}
}
