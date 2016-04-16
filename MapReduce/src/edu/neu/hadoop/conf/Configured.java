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
}
