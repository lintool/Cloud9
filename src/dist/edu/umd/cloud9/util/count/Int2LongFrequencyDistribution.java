package edu.umd.cloud9.util.count;

import java.util.List;

import edu.umd.cloud9.io.pair.PairOfIntLong;

/**
 * A frequency distribution where events are ints and counts are longs.
 *
 * @author Jimmy Lin
 *
 */
public interface Int2LongFrequencyDistribution extends Iterable<PairOfIntLong> {

	/**
	 * Increments the frequency of an event <code>key</code>.
	 */
	public void increment(int key);

	/**
	 * Increments the frequency of an event <code>key</code> by <code>cnt</code>.
	 */
	public void increment(int key, long cnt);

	/**
	 * Decrements the frequency of an event <code>key</code>.
	 */
	public void decrement(int key);

	/**
	 * Decrements the frequency of a particular event <code>key</code> by <code>cnt</code>.
	 */
	public void decrement(int key, long cnt);

	/**
	 * Returns true if <i>key</i> exists in this object.
	 */
	public boolean contains(int key);

	/**
	 * Returns the frequency of a particular event <i>key</i>.
	 */
	public long get(int key);

	/**
	 * Sets the frequency of a particular event <i>key</i> to <code>cnt</code>.
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
	 * Returns events sorted by frequency of occurrence.
	 */
	public List<PairOfIntLong> getFrequencySortedEvents();

	/**
	 * Returns top <i>n</i> events sorted by frequency of occurrence.
	 */
	public List<PairOfIntLong> getFrequencySortedEvents(int n);

	/**
	 * Returns events in sorted order.
	 */
	public List<PairOfIntLong> getSortedEvents();

	/**
	 * Returns top <i>n</i> events in sorted order.
	 */
	public List<PairOfIntLong> getSortedEvents(int n);

	/**
	 * Returns number of distinct events observed. Note that if an event is
	 * observed and then its count subsequently removed, the event will not be
	 * included in this count.
	 */
	public int getNumberOfEvents();

	/**
	 * Returns the sum of frequencies of all observed events.
	 */
	public long getSumOfFrequencies();
}
