package edu.umd.cloud9.util.count;

import edu.umd.cloud9.util.SortableEntries;
import edu.umd.cloud9.util.pair.PairOfObjectLong;

/**
 * A frequency distribution where events are arbitrary objects and counts are ints.
 * 
 * @author Jimmy Lin
 * 
 */
public interface Object2LongFrequencyDistribution<K extends Comparable<K>>
    extends SortableEntries<PairOfObjectLong<K>> {

	/**
	 * Increments the count of an event <code>key</code>.
	 */
	public void increment(K key);

	/**
	 * Increments the count of an event <code>key</code> by <code>cnt</code>.
	 */
	public void increment(K key, long cnt);

	/**
	 * Decrements the count of an event <code>key</code>.
	 */
	public void decrement(K key);

	/**
	 * Decrements the count of a particular event <code>key</code> by <code>cnt</code>.
	 */
	public void decrement(K key, long cnt);

	/**
	 * Returns true if <i>key</i> exists in this object.
	 */
	public boolean contains(K key);

	/**
	 * Returns the count of a particular event <i>key</i>.
	 */
	public long get(K key);

  /**
   * Returns the frequency of a particular event <i>key</i>.
   */
  public float getFrequency(K key);

  /**
   * Returns the log frequency of a particular event <i>key</i>.
   */
  public float getLogFrequency(K key);

	/**
	 * Sets the count of a particular event <i>key</i> to <code>cnt</code>.
	 */
	public long set(K key, long cnt);

	/**
	 * Removes the count of a particular event <code>key</code>.
	 */
	public long remove(K k);

	/**
	 * Removes all events.
	 */
	public void clear();

	/**
	 * Returns number of distinct events observed. Note that if an event is
	 * observed and then its count subsequently removed, the event will not be
	 * included in this count.
	 */
	public int getNumberOfEvents();

	/**
	 * Returns the sum of counts of all observed events.
	 */
	public long getSumOfCounts();
}
