package edu.neu.hadoop.mapreduce;

/**
 * Partitions the key space.
 * 
 * <p><code>Partitioner</code> controls the partitioning of the keys of the 
 * intermediate map-outputs. The key (or a subset of the key) is used to derive
 * the partition, typically by a hash function. The total number of partitions
 * is the same as the number of reduce tasks for the job. Hence this controls
 * which of the <code>m</code> reduce tasks the intermediate key (and hence the 
 * record) is sent for reduction.</p>
 * 
 * @author Adib Alwani
 */
public abstract class Partitioner<KEY, VALUE> {

	/**
	 * Get the partition number for a given key (hence record) given the total
	 * number of partitions i.e. number of reduce-tasks for the job.
	 * 
	 * <p>
	 * Typically a hash function on a all or a subset of the key.
	 * </p>
	 * 
	 * @param key the key to be partioned.
	 * @param value the entry value.
	 * @param numPartitions the total number of partitions.
	 * @return the partition number for the <code>key</code>.
	 */
	public abstract int getPartition(KEY key, VALUE value, int numPartitions);

}
