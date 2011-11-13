package edu.umd.cloud9.util.fd;

import edu.umd.cloud9.io.pair.PairOfIntLong;
import edu.umd.cloud9.util.SortableEntries;

/**
 * A frequency distribution where events are ints and counts are longs.
 *
 * @author Jimmy Lin
 */
public interface Int2LongFrequencyDistribution extends SortableEntries<PairOfIntLong> {

	/**
	 * Increments the count of an event <code>key</code>.
	 */
	public void increment(int key);

	/**
	 * Increments the count of an event <code>key</code> by <code>cnt</code>.
	 */
	public void increment(int key, long cnt);

	/**
	 * Decrements the count of an event <code>key</code>.
	 */
	public void decrement(int key);

	/**
	 * Decrements the count of a particular event <code>key</code> by <code>cnt</code>.
	 */
	public void decrement(int key, long cnt);

	/**
	 * Returns true if <i>key</i> exists in this object.
	 */
	public boolean contains(int key);

	/**
	 * Returns the count of a particular event <i>key</i>.
	 */
	public long get(int key);

  /**
   * Returns the frequency of a particular event <i>key</i>.
   */
  public float getFrequency(int key);

  /**
   * Returns the log frequency of a particular event <i>key</i>.
   */
  public float getLogFrequency(int key);

	/**
	 * Sets the count of a particular event <i>key</i> to <code>cnt</code>.
	 */
	public long set(int key, long cnt);

	/**
	 * Removes the count of a particular event <code>key</code>.
	 */
	public long remove(int k);

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
