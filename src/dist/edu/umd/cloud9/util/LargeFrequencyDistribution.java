/*
 * Cloud9: A MapReduce Library for Hadoop
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package edu.umd.cloud9.util;

import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.google.common.collect.Lists;

import edu.umd.cloud9.io.PairOfStringLong;

/**
 * Similar to {@link FrequencyDistribution}, but keep tracks of counts
 * with longs. Thus, useful keeping track of distributions with a large number
 * of observations.
 *
 * @author Jimmy Lin
 *
 */
public class LargeFrequencyDistribution<K> extends Object2LongOpenHashMap<K> {

	private static final long serialVersionUID = -5283249239701824488L;

	private long mSumOfFrequencies = 0;

	/**
	 * Increments the frequency of an event <code>key</code>.
	 */
	public void increment(K key) {
		if (containsKey(key)) {
			put(key, get(key) + 1L);
		} else {
			put(key, 1L);
		}
	}

	/**
	 * Increments the frequency of an event <code>key</code> by <code>cnt</code>
	 * .
	 */
	public void increment(K key, long cnt) {
		if (containsKey(key)) {
			put(key, get(key) + cnt);
		} else {
			put(key, cnt);
		}
	}

	/**
	 * Decrements the frequency of an event <code>key</code>.
	 */
	public void decrement(K key) {
		if (containsKey(key)) {
			long v = get(key);
			if (v == 1) {
				remove(key);
			} else {
				put(key, this.get(key) - 1L);
			}
		} else {
			throw new RuntimeException("Can't decrement non-existent event!");
		}
	}

	/**
	 * Sets the frequency of a particular event <code>key</code> to count
	 * <code>v</code>.
	 */
	public void decrement(K key, long cnt) {
		if (containsKey(key)) {
			long v = get(key);
			if (v < cnt) {
				throw new RuntimeException("Can't decrement past zero!");
			} else if (v == cnt) {
				remove(key);
			} else {
				put(key, this.get(key) - cnt);
			}
		} else {
			throw new RuntimeException("Can't decrement non-existent event!");
		}
	}

	/**
	 * Returns the frequency of a particular event <i>key</i>.
	 */
	public long get(K key) {
		return getLong(key);
	}

	/**
	 * Sets the frequency of a particular event <code>key</code> to count
	 * <code>v</code>.
	 */
	@Override
	public long put(K k, long v) {
		long rv = super.put(k, v);
		mSumOfFrequencies = mSumOfFrequencies - rv + v;

		return rv;
	}

	/**
	 * Sets the frequency of a particular event <code>ok</code> to count
	 * <code>ov</code>.
	 */
	@Override
	public Long put(K ok, Long ov) {
		return put(ok, (long) ov);
	}

	/**
	 * Removes the count of a particular event <code>key</code>.
	 */
	public long remove(K k) {
		long rv = super.remove(k);
		mSumOfFrequencies -= rv;

		return rv;
	}

	/**
	 * Removes the count of a particular event <code>ok</code>.
	 */
	@Override
	@SuppressWarnings("unchecked")
	public long removeLong(final Object ok) {
		return remove((K) ok);
	}

	/**
	 * Returns events sorted by frequency of occurrence.
	 */
	public List<PairOfStringLong> getFrequencySortedEvents() {
		List<PairOfStringLong> list = Lists.newArrayList();

		for (Object2LongMap.Entry<K> e : object2LongEntrySet()) {
			list.add(new PairOfStringLong((String) e.getKey(), e.getLongValue()));
		}

		Collections.sort(list, new Comparator<PairOfStringLong>() {
			public int compare(PairOfStringLong e1, PairOfStringLong e2) {
				if (e1.getRightElement() > e2.getRightElement()) {
					return -1;
				}

				if (e1.getRightElement() < e2.getRightElement()) {
					return 1;
				}

				return e1.getLeftElement().compareTo(e2.getLeftElement());
			}
		});

		return list;
	}

	/**
	 * Returns top <i>n</i> events sorted by frequency of occurrence.
	 */
	public List<PairOfStringLong> getFrequencySortedEvents(int n) {
		List<PairOfStringLong> list = getFrequencySortedEvents();
		return list.subList(0, n);
	}

	/**
	 * Returns events in sorted order.
	 */
	public List<PairOfStringLong> getSortedEvents() {
		List<PairOfStringLong> list = Lists.newArrayList();

		for (Object2LongMap.Entry<K> e : object2LongEntrySet()) {
			list.add(new PairOfStringLong((String) e.getKey(), e.getLongValue()));
		}

		// sort the entries
		Collections.sort(list, new Comparator<PairOfStringLong>() {
			public int compare(PairOfStringLong e1, PairOfStringLong e2) {
				if (e1.getLeftElement().equals(e2.getLeftElement())) {
					throw new RuntimeException("Event observed twice!");
				}

				return e1.getLeftElement().compareTo(e1.getLeftElement());
			}
		});

		return list;
	}

	/**
	 * Returns top <i>n</i> events in sorted order.
	 */
	public List<PairOfStringLong> getSortedEvents(int n) {
		List<PairOfStringLong> list = getSortedEvents();
		return list.subList(0, n);
	}

	/**
	 * Returns number of distinct events observed. Note that if an event is
	 * observed and then its count subsequently removed, the event will not be
	 * included in this count.
	 */
	public int getNumberOfEvents() {
		return size();
	}

	/**
	 * Returns the sum of frequencies of all observed events.
	 */
	public long getSumOfFrequencies() {
		return mSumOfFrequencies;
	}
}
