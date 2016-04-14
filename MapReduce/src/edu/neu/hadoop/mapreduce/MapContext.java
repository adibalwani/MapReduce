package edu.neu.hadoop.mapreduce;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import edu.neu.hadoop.conf.Configuration;
import edu.neu.hadoop.io.Text;
import edu.neu.hadoop.mapreduce.lib.output.Writer;
import edu.neu.hadoop.mapreduce.lib.partition.HashPartitioner;


/**
 * The <code>Context</code> passed on to the {@link Mapper} implementation
 * 
 * @author Adib Alwani
 */
@SuppressWarnings({"rawtypes", "unchecked", "unused"})
public class MapContext extends Context {
	
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

	@Override
	public <KEYOUT, VALUEOUT> void write(KEYOUT key, VALUEOUT value) {
		List<KeyValue> list = partitions.get(
			partitioner.getPartition(key, value, numPartitions)
		);
		list.add(new KeyValue(key, value));
		
	}
	
	/**
	 * Sort individual partitions
	 */
	public void sortPartitions() {
		for (List<KeyValue> partition : partitions) {
			Collections.sort(partition);
		}
	}
	
	/**
	 * Spill individual partitions to disk
	 */
	public void spillToDisk() {
		Writer writer = new Writer();
		for (int i = 0; i < numPartitions; i++) {
			writer.write(partitions.get(i), i, Thread.currentThread().getName());
		}
	}
}
