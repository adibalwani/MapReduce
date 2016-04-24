package edu.neu.hadoop.mapreduce;


/**
 * <p><code>Counters</code> holds per job/task counters, defined either by the
 * Map-Reduce framework or applications. Each <code>Counter</code> can be of
 * any {@link Enum} type.</p>
 * 
 * @author Adib Alwani
 */
public class Counters {
	
	public static long MAP_OUTPUT_RECORDS = 0;
	
	/**
	 * Get the counter's group
	 * 
	 * @param name Counter name
	 * @return {@link Counters}
	 */
	public Counters getGroup(String name) {
		return this;
	}
	
	/**
	 * Find the counter
	 * 
	 * @param counter {@link TaskCounter} name
	 * @param name Counter name
	 * @return {@link Counters}
	 */
	public Counters findCounter(String counter, String name) {
		return this;
	}
	
	/**
	 * Get the {@link TaskCounter} value
	 * 
	 * @return Value
	 */
	public long getValue() {
		return MAP_OUTPUT_RECORDS;
	}
}
