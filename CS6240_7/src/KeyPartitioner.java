import org.apache.hadoop.mapreduce.Partitioner;


public class KeyPartitioner extends Partitioner<Key, FlightDetailModelPair> {

	@Override
	public int getPartition(Key key, FlightDetailModelPair pair, int numPartitions) {
		return key.getKey().hashCode() % numPartitions;
	}
}
