package edu.neu.hadoop.io;

/**
 * A {@link Writable} which is also {@link Comparable}. 
 *
 * <p><code>WritableComparable</code>s can be compared to each other, typically 
 * via <code>Comparator</code>s. Any type which is to be used as a 
 * <code>key</code> in the Hadoop Map-Reduce framework should implement this
 * interface.</p>
 * 
 * <p>Note that <code>hashCode()</code> is frequently used in Hadoop to partition
 * keys. It's important that your implementation of hashCode() returns the same 
 * result across different instances of the JVM. Note also that the default 
 * <code>hashCode()</code> implementation in <code>Object</code> does <b>not</b>
 * satisfy this property.</p>
 * 
 * @author Adib Alwani
 */
public interface WritableComparable<T> extends Writable, Comparable<T> { }
