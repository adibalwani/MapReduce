package edu.neu.hadoop.mapreduce;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import edu.neu.hadoop.conf.Configuration;
import edu.neu.hadoop.io.Text;
import edu.neu.hadoop.mapreduce.lib.output.Writer;
import edu.neu.hadoop.mapreduce.lib.partition.HashPartitioner;
import edu.neu.hadoop.mapreduce.network.HostNameManager;


/**
 * The <code>Context</code> passed on to the {@link Mapper} implementation
 * 
 * @author Adib Alwani
 */
@SuppressWarnings({"rawtypes", "unchecked", "unused"})
public class MapContext extends Context {
	
	private Configuration conf;
	private List<List<KeyValue>> partitions;
	private List<Integer> partitionsIds;
	Partitioner partitioner;
	private int numPartitions;
	
	public MapContext(Configuration conf) {
		this.conf = conf;
		inputPaths = conf.getInputPath();
		numPartitions = conf.getNumReduceTasks();
		partitions = new ArrayList<List<KeyValue>>();
		partitionsIds = new ArrayList<Integer>();
		partitioner = new HashPartitioner<>();
		for (int i = 0; i < numPartitions; i++) {
			partitions.add(new ArrayList<KeyValue>());
			partitionsIds.add(0);
		}
	}

	@Override
	public <KEYOUT, VALUEOUT> void write(KEYOUT key, VALUEOUT value) {
		int numPartition = partitioner.getPartition(key, value, numPartitions);
		List<KeyValue> list = partitions.get(numPartition);
		list.add(new KeyValue(key, value));
		if (list.size() > Constants.MAPPER_BUFFER_CAPACITY) {
			sortPartitions(list);
			spillToDisk(list, numPartition);
			list.clear();
		}
	}
	
	/**
	 * Sort the provided partition
	 * 
	 * @param partition Partition
	 */
	private void sortPartitions(List<KeyValue> partition) {
		Collections.sort(partition);
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
	 * Spill the provided partition to disk
	 * 
	 * @param partition Partition
	 * @param numPartition Partition number
	 */
	private void spillToDisk(List<KeyValue> partition, int numPartition) {
		System.out.println("Buffer full. Spilled to disk");
		Writer writer = new Writer();
		boolean clusterMode = conf.getOutputPath().getPath().contains("s3");
		if (clusterMode) {
			HostNameManager hostNameManager = new HostNameManager();
			writer.write(partition, numPartition, hostNameManager.getOwnHostName());
		} else {
			int index = partitionsIds.get(numPartition);
			String fileName = Thread.currentThread().getName() + index;
			writer.write(partition, numPartition, fileName);
			partitionsIds.set(numPartition, index + 1);
		}
	}
	
	/**
	 * Spill individual partitions to disk
	 */
	public void spillToDisk() {
		Writer writer = new Writer();
		HostNameManager hostNameManager = new HostNameManager();
		for (int i = 0; i < numPartitions; i++) {
			if (!partitions.get(i).isEmpty()) {
				boolean clusterMode = conf.getOutputPath().getPath().contains("s3");
				if (clusterMode) {
					writer.write(partitions.get(i), i, hostNameManager.getOwnHostName());
				} else {
					writer.write(partitions.get(i), i, Thread.currentThread().getName());
				}
			}
		}
	}
}
