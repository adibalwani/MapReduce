package edu.neu.hadoop.mapreduce;

import java.util.ArrayList;
import java.util.List;

import edu.neu.hadoop.conf.Configuration;
import edu.neu.hadoop.mapreduce.lib.partition.HashPartitioner;


/**
 * The <code>Context</code> passed on to the {@link Mapper} implementation
 * 
 * @author Adib Alwani
 */
@SuppressWarnings("rawtypes")
public class MapContext extends Context {
	
	@SuppressWarnings("unused")
	private Configuration conf;
	private List<List<KeyValue>> partitions;
	Partitioner partitioner;
	private int numPartitions;
	
	public MapContext(Configuration conf) {
		this.conf = conf;
		inputPaths = conf.getInputPath();
		numPartitions = conf.getNumReduceTasks();
		partitions = new ArrayList<List<KeyValue>>();
		partitioner = new HashPartitioner<>();
		for (int i = 0; i < numPartitions; i++) {
			partitions.add(new ArrayList<KeyValue>());
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <KEYOUT, VALUEOUT> void write(KEYOUT key, VALUEOUT value) {
		List<KeyValue> list = partitions.get(
				partitioner.getPartition(key, value, numPartitions)
		);
		list.add(new KeyValue(key, value));
	}
	
	/**
	 * POJO to store emitted key-value pair
	 * 
	 * @author Adib Alwani
	 */
	static class KeyValue<KEY, VALUE> implements Comparable<KEY> {
		KEY key;
		VALUE value;
		
		public KeyValue(KEY key, VALUE value) {
			this.key = key;
			this.value = value;
		}

		@Override
		public int compareTo(KEY o) {
			return key.compareTo(0);
		}
	}
}
