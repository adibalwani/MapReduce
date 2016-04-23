package edu.neu.hadoop.mapreduce.lib.partition;

import edu.neu.hadoop.mapreduce.Partitioner;


/**
 * Partition keys by their {@link Object#hashCode()}.
 * 
 * @author Adib Alwani
 */
public class HashPartitioner<K, V> extends Partitioner<K, V> {

	/** Use {@link Object#hashCode()} to partition. */
	public int getPartition(K key, V value, int numReduceTasks) {
		return (key.toString().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
	}
}
