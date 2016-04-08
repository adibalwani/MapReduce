package edu.neu.hadoop.mapreduce;

import edu.neu.hadoop.conf.Configuration;


/**
 * The job submitter's view of the Job.
 * 
 * <p>It allows the user to configure the
 * job and submit it. The set methods only work until the job is submitted</p>
 * 
 * @author Adib Alwani
 */
@SuppressWarnings("rawtypes")
public class Job {
	
	private Configuration conf;
	
	private Job() { }
	
	/**
	 * Creates a new {@link Job} and a given {@link Configuration}.
	 * 
	 * The <code>Job</code> makes a copy of the <code>Configuration</code> so
	 * that any necessary internal modifications do not reflect on the incoming
	 * parameter.
	 * 
	 * @param conf the configuration
	 * @return the {@link Job} , with no connection to a cluster yet.
	 */
	public static Job getInstance(Configuration conf) {
		Job job = new Job();
		job.conf = conf;
		return job;
	}
	
	/**
	 * Creates a new {@link Job} and a given {@link Configuration}.
	 * 
	 * The <code>Job</code> makes a copy of the <code>Configuration</code> so
	 * that any necessary internal modifications do not reflect on the incoming
	 * parameter.
	 * 
	 * @param conf the configuration
	 * @param jobName Name of the job 
	 * @return the {@link Job} , with no connection to a cluster yet.
	 */
	public static Job getInstance(Configuration conf, String jobName) {
		return getInstance(conf);
	}
	
	/**
	 * Set the Jar by finding where a given class came from.
	 * 
	 * @param cls the example class
	 */
	public void setJarByClass(Class<?> cls) {
		conf.setJarByClass(cls);
	}

	/**
	 * Set the job jar
	 */
	public void setJar(String jar) {
		conf.setJar(jar);
	}
	
	/**
	 * Set the {@link Mapper} for the job.
	 * 
	 * @param cls the <code>Mapper</code> to use
	 */
	public void setMapperClass(Class<? extends Mapper> cls)
			throws IllegalStateException {
		conf.setMapperClass(cls);
	}
	
	/**
	 * Set the key class for the job output data.
	 * 
	 * @param theClass the key class for the job output data.
	 */
	public void setOutputKeyClass(Class<?> theClass)
			throws IllegalStateException {
		conf.setOutputKeyClass(theClass);
	}

	/**
	 * Set the value class for job outputs.
	 * 
	 * @param theClass the value class for job outputs.
	 */
	public void setOutputValueClass(Class<?> theClass)
			throws IllegalStateException {
		conf.setOutputValueClass(theClass);
	}
	
	/**
	 * Set the key class for the map output data. This allows the user to
	 * specify the map output key class to be different than the final output
	 * value class.
	 * 
	 * @param theClass the map output key class.
	 */
	public void setMapOutputKeyClass(Class<?> theClass)
			throws IllegalStateException {
		conf.setMapOutputKeyClass(theClass);
	}

	/**
	 * Set the value class for the map output data. This allows the user to
	 * specify the map output value class to be different than the final output
	 * value class.
	 * 
	 * @param theClass the map output value class.
	 */
	public void setMapOutputValueClass(Class<?> theClass)
			throws IllegalStateException {
		conf.setMapOutputValueClass(theClass);
	}
	
	/**
	 * Submit the job to the cluster and wait for it to finish.
	 * 
	 * @param verbose print the progress to the user
	 * @return true if the job succeeded
	 */
	public boolean waitForCompletion(boolean verbose) throws ClassNotFoundException {
		Master master = new Master(conf);
		return master.submitJob();
	}
	
}
