package edu.neu.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A serializable object which implements a simple, efficient, serialization 
 * protocol, based on {@link DataInput} and {@link DataOutput}
 * 
 * <p>Any <code>key</code> or <code>value</code> type in the Hadoop Map-Reduce
 * framework implements this interface.</p>
 * 
 * <p>Implementations typically implement a static <code>read(DataInput)</code>
 * method which constructs a new instance, calls {@link #readFields(DataInput)} 
 * and returns the instance.</p>
 * 
 * @author Adib Alwani
 */
public abstract class Writable implements Cloneable {

	/**
	 * Serialize the fields of this object to <code>out</code>
	 * 
	 * @param out <code>DataOuput</code> to serialize this object into
	 * @throws IOException
	 */
	public void write(DataOutput out) throws IOException { }

	/**
	 * Deserialize the fields of this object from <code>in</code>
	 * 
	 * @param in <code>DataInput</code> to deserialize this object from
	 * @throws IOException
	 */
	public void readFields(DataInput in) throws IOException { }
	
	@Override
	public Object clone() throws CloneNotSupportedException {
		return super.clone();
	}
}
