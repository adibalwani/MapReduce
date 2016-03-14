import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class KeyComparator extends WritableComparator {
	
	protected KeyComparator() {
		super(Key.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable pair1, WritableComparable pair2) {
		Key key1 = (Key) pair1;
		Key key2 = (Key) pair2;
		return key1.getKey().compareTo(key2.getKey());
	}
}
