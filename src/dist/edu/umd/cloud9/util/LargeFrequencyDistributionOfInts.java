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

import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.google.common.collect.Lists;

import edu.umd.cloud9.io.PairOfIntLong;

/**
 * Similar to {@link FrequencyDistributionOfInts}, but keep tracks of counts
 * with longs. Thus, useful keeping track of distributions with a large number
 * of observations.
 *
 * @author Jimmy Lin
 *
 */
public class LargeFrequencyDistributionOfInts extends Int2LongOpenHashMap {

	private static final long serialVersionUID = 2937102257008737066L;

	private long mSumOfFrequencies = 0;

	/**
	 * Increments the frequency of an event <code>key</code>.
	 */
	public void increment(int key) {
		if (containsKey(key)) {
			put(key, get(key) + 1L);
		} else {
			put(key, 1L);
		}
	}

	/**
	 * Increments the frequency of an event <code>key</code> by <code>cnt</code>.
	 */
	public void increment(int key, long cnt) {
		if (containsKey(key)) {
			put(key, get(key) + cnt);
		} else {
			put(key, cnt);
		}
	}

	/**
	 * Decrements the frequency of an event <code>key</code>.
	 */
	public void decrement(int key) {
		if (containsKey(key)) {
			long v = get(key);
			if (v == 1L) {
				remove(key);
			} else {
				put(key, this.get(key) - 1L);
			}
		} else {
			throw new RuntimeException("Can't decrement non-existent event!");
		}
	}

	/**
	 * Decrements the frequency of an event <code>key</code> by <code>cnt</code>.
	 */
	public void decrement(int key, long cnt) {
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
	@Override
	public long get(int key) {
		return super.get(key);
	}

	/**
	 * Sets the frequency of a particular event <code>key</code> to count <code>v</code>.
	 */
	@Override
	public long put(int k, long v) {
		long rv = super.put(k, v);
		mSumOfFrequencies = mSumOfFrequencies - rv + v;

		return rv;
	}

	/**
	 * Sets the frequency of a particular event <code>ok</code> to count <code>ov</code>.
	 */
	@Override
	public Long put(Integer ok, Long ov) {
		return put((int) ok, (long) ov);
	}

	/**
	 * Removes the count of a particular event <code>key</code>.
	 */
	@Override
	public long remove(int k) {
		long rv = super.remove(k);
		mSumOfFrequencies -= rv;

		return rv;
	}

	/**
	 * Removes the count of a particular event <code>ok</code>.
	 */
	@Override
	public Long remove(Object ok) {
		return this.remove((int) (Integer) ok);
	}

	/**
	 * Returns events sorted by frequency of occurrence.
	 */
	public List<PairOfIntLong> getFrequencySortedEvents() {
		List<PairOfIntLong> list = Lists.newArrayList();

		for (Int2LongMap.Entry e : int2LongEntrySet()) {
			list.add(new PairOfIntLong(e.getIntKey(), e.getLongValue()));
		}

		Collections.sort(list, new Comparator<PairOfIntLong>() {
			public int compare(PairOfIntLong e1, PairOfIntLong e2) {
				if (e1.getRightElement() > e2.getRightElement()) {
					return -1;
				}

				if (e1.getRightElement() < e2.getRightElement()) {
					return 1;
				}

				if (e1.getLeftElement() == e2.getLeftElement()) {
					throw new RuntimeException("Event observed twice!");
				}

				return e1.getLeftElement() < e2.getLeftElement() ? -1 : 1;
			}
		});

		return list;
	}

	/**
	 * Returns top <i>n</i> events sorted by frequency of occurrence.
	 */
	public List<PairOfIntLong> getFrequencySortedEvents(int n) {
		List<PairOfIntLong> list = getFrequencySortedEvents();
		return list.subList(0, n);
	}

	/**
	 * Returns events in sorted order.
	 */
	public List<PairOfIntLong> getSortedEvents() {
		List<PairOfIntLong> list = Lists.newArrayList();

		for (Int2LongMap.Entry e : int2LongEntrySet()) {
			list.add(new PairOfIntLong(e.getIntKey(), e.getLongValue()));
		}

		Collections.sort(list, new Comparator<PairOfIntLong>() {
			public int compare(PairOfIntLong e1, PairOfIntLong e2) {
				if (e1.getLeftElement() > e2.getLeftElement()) {
					return 1;
				}

				if (e1.getLeftElement() < e2.getLeftElement()) {
					return -1;
				}

				throw new RuntimeException("Event observed twice!");
			}
		});

		return list;
	}

	/**
	 * Returns top <i>n</i> events in sorted order.
	 */
	public List<PairOfIntLong> getSortedEvents(int n) {
		List<PairOfIntLong> list = getSortedEvents();
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
