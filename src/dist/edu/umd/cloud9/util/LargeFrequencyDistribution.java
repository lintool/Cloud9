package edu.umd.cloud9.util;

import java.util.List;

/**
 * Similar to {@link FrequencyDistribution}, but keep tracks of counts with
 * longs. Thus, useful keeping track of distributions with a large number
 * of observations.
 *
 * @author Jimmy Lin
 *
 */
public interface LargeFrequencyDistribution<K extends Comparable<K>> {

	/**
	 * Increments the frequency of an event <code>key</code>.
	 */
	public void increment(K key);

	/**
	 * Increments the frequency of an event <code>key</code> by <code>cnt</code>.
	 */
	public void increment(K key, long cnt);

	/**
	 * Decrements the frequency of an event <code>key</code>.
	 */
	public void decrement(K key);

	/**
	 * Decrements the frequency of a particular event <code>key</code> by <code>cnt</code>.
	 */
	public void decrement(K key, long cnt);

	/**
	 * Returns true if <i>key</i> exists in this object.
	 */
	public boolean contains(K key);

	/**
	 * Returns the frequency of a particular event <i>key</i>.
	 */
	public long get(K key);

	/**
	 * Sets the frequency of a particular event <i>key</i> to <code>cnt</code>.
	 */
	public long set(K key, long cnt);

	/**
	 * Removes the count of a particular event <code>key</code>.
	 */
	public long remove(K k);

	/**
	 * Returns events sorted by frequency of occurrence.
	 */
	public List<PairOfObjectLong<K>> getFrequencySortedEvents();

	/**
	 * Returns top <i>n</i> events sorted by frequency of occurrence.
	 */
	public List<PairOfObjectLong<K>> getFrequencySortedEvents(int n);

	/**
	 * Returns events in sorted order.
	 */
	public List<PairOfObjectLong<K>> getSortedEvents();

	/**
	 * Returns top <i>n</i> events in sorted order.
	 */
	public List<PairOfObjectLong<K>> getSortedEvents(int n);

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
