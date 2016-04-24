package edu.neu.hadoop.conf;

/**
 * Base class for things that may be configured with a {@link Configuration}
 * 
 * @author Adib Alwani
 */
public class Configured implements Configurable {

	/**
	 * Return a {@link Configuration} object
	 */
	public Configuration getConf() {
		return new Configuration();
	}

	/**
	 * Set the {@link Configuration} object
	 * 
	 * @param conf {@link Configuration} object
	 */
	public void setConf(Configuration conf) {
		// Do Nothing
	}
}
