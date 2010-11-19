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

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.google.common.collect.Lists;

import edu.umd.cloud9.io.PairOfStringInt;

/**
 * An implementation of a frequency distribution for arbitrary events, backed by
 * a fastutil map. One common use is to store frequency counts for language
 * models, where the events are Strings. This class keeps track of frequencies
 * using ints, so beware when dealing with a large number of observations; see
 * also {@link LargeFrequencyDistribution}.
 *
 * @author Jimmy Lin
 *
 */
public class FrequencyDistribution<K> extends Object2IntOpenHashMap<K> {

	private static final long serialVersionUID = -1167146372606430678L;

	private long mSumOfFrequencies = 0;

	/**
	 * Increments the frequency of an event <code>key</code>.
	 */
	public void increment(K key) {
		if (containsKey(key)) {
			put(key, get(key) + 1);
		} else {
			put(key, 1);
		}
	}

	/**
	 * Increments the frequency of an event <code>key</code> by <code>cnt</code>.
	 */
	public void increment(K key, int cnt) {
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
			int v = get(key);
			if (v == 1) {
				remove(key);
			} else {
				put(key, this.get(key) - 1);
			}
		} else {
			throw new RuntimeException("Can't decrement non-existent event!");
		}
	}

	/**
	 * Sets the frequency of a particular event <code>key</code> to count <code>v</code>.
	 */
	public void decrement(K key, int cnt) {
		if (containsKey(key)) {
			int v = get(key);
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
	public int get(K key) {
		return getInt(key);
	}

	/**
	 * Sets the frequency of a particular event <code>key</code> to count <code>v</code>.
	 */
	@Override
	public int put(K k, int v) {
		int rv = super.put(k, v);
		mSumOfFrequencies = mSumOfFrequencies - rv + v;

		return rv;
	}

	/**
	 * Sets the frequency of a particular event <code>ok</code> to count <code>ov</code>.
	 */
	@Override
	public Integer put(K ok, Integer ov) {
		return put(ok, (int) ov);
	}

	/**
	 * Removes the count of a particular event <code>key</code>.
	 */
	public int remove(K k) {
		int rv = super.remove(k);
		mSumOfFrequencies -= rv;

		return rv;
	}

	/**
	 * Removes the count of a particular event <code>ok</code>.
	 */
	@Override @SuppressWarnings("unchecked")
	public int removeInt(final Object ok) {
		return remove((K) ok);
	}

	/**
	 * Returns events sorted by frequency of occurrence.
	 */
	public List<PairOfStringInt> getFrequencySortedEvents() {
		List<PairOfStringInt> list = Lists.newArrayList();

		for (Object2IntMap.Entry<K> e : object2IntEntrySet()) {
			list.add(new PairOfStringInt((String) e.getKey(), e.getIntValue()));
		}

		Collections.sort(list, new Comparator<PairOfStringInt>() {
			public int compare(PairOfStringInt e1, PairOfStringInt e2) {
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
	public List<PairOfStringInt> getFrequencySortedEvents(int n) {
		List<PairOfStringInt> list = getFrequencySortedEvents();
		return list.subList(0, n);
	}

	/**
	 * Returns events in sorted order.
	 */
	public List<PairOfStringInt> getSortedEvents() {
		List<PairOfStringInt> list = Lists.newArrayList();

		for (Object2IntMap.Entry<K> e : object2IntEntrySet()) {
			list.add(new PairOfStringInt((String) e.getKey(), e.getIntValue()));
		}

		// sort the entries
		Collections.sort(list, new Comparator<PairOfStringInt>() {
			public int compare(PairOfStringInt e1, PairOfStringInt e2) {
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
	public List<PairOfStringInt> getSortedEvents(int n) {
		List<PairOfStringInt> list = getSortedEvents();
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
