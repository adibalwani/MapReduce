package edu.neu.hadoop.conf;

/**
 * Something that may be configured with a {@link Configuration}.
 * 
 * @author Adib Alwani
 */
public interface Configurable {
	
	/** Return the configuration used by this object. */
	Configuration getConf();
}
