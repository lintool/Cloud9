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

/**
 * Similar to {@link OpenFrequencyDistribution}, but keep tracks of counts
 * with longs. Thus, useful keeping track of distributions with a large number
 * of observations.
 *
 * @author Jimmy Lin
 *
 */
public class OpenLargeFrequencyDistribution<K extends Comparable<K>> implements LargeFrequencyDistribution<K> {

	private Object2LongOpenHashMap<K> mCounts = new Object2LongOpenHashMap<K>();
	private long mSumOfFrequencies = 0;

	@Override
	public void increment(K key) {
		if (contains(key)) {
			set(key, get(key) + 1L);
		} else {
			set(key, 1L);
		}
	}

	@Override
	public void increment(K key, long cnt) {
		if (contains(key)) {
			set(key, get(key) + cnt);
		} else {
			set(key, cnt);
		}
	}

	@Override
	public void decrement(K key) {
		if (contains(key)) {
			long v = get(key);
			if (v == 1) {
				remove(key);
			} else {
				set(key, v - 1L);
			}
		} else {
			throw new RuntimeException("Can't decrement non-existent event!");
		}
	}

	@Override
	public void decrement(K key, long cnt) {
		if (contains(key)) {
			long v = get(key);
			if (v < cnt) {
				throw new RuntimeException("Can't decrement past zero!");
			} else if (v == cnt) {
				remove(key);
			} else {
				set(key, v - cnt);
			}
		} else {
			throw new RuntimeException("Can't decrement non-existent event!");
		}
	}

	@Override
	public boolean contains(K key) {
		return mCounts.containsKey(key);
	}

	@Override
	public long get(K key) {
		return mCounts.getLong(key);
	}

	@Override
	public long set(K k, long v) {
		long rv = mCounts.put(k, v);
		mSumOfFrequencies = mSumOfFrequencies - rv + v;

		return rv;
	}

	@Override
	public long remove(K k) {
		long rv = mCounts.remove(k);
		mSumOfFrequencies -= rv;

		return rv;
	}

	@Override
	public List<PairOfObjectLong<K>> getFrequencySortedEvents() {
		List<PairOfObjectLong<K>> list = Lists.newArrayList();

		for (Object2LongMap.Entry<K> e : mCounts.object2LongEntrySet()) {
			list.add(new PairOfObjectLong<K>(e.getKey(), e.getLongValue()));
		}

		Collections.sort(list, new Comparator<PairOfObjectLong<K>>() {
			public int compare(PairOfObjectLong<K> e1, PairOfObjectLong<K> e2) {
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

	@Override
	public List<PairOfObjectLong<K>> getFrequencySortedEvents(int n) {
		List<PairOfObjectLong<K>> list = getFrequencySortedEvents();
		return list.subList(0, n);
	}

	@Override
	public List<PairOfObjectLong<K>> getSortedEvents() {
		List<PairOfObjectLong<K>> list = Lists.newArrayList();

		for (Object2LongMap.Entry<K> e : mCounts.object2LongEntrySet()) {
			list.add(new PairOfObjectLong<K>(e.getKey(), e.getLongValue()));
		}

		// sort the entries
		Collections.sort(list, new Comparator<PairOfObjectLong<K>>() {
			public int compare(PairOfObjectLong<K> e1, PairOfObjectLong<K> e2) {
				if (e1.getLeftElement().equals(e2.getLeftElement())) {
					throw new RuntimeException("Event observed twice!");
				}

				return e1.getLeftElement().compareTo(e1.getLeftElement());
			}
		});

		return list;
	}

	@Override
	public List<PairOfObjectLong<K>> getSortedEvents(int n) {
		List<PairOfObjectLong<K>> list = getSortedEvents();
		return list.subList(0, n);
	}

	@Override
	public int getNumberOfEvents() {
		return mCounts.size();
	}

	@Override
	public long getSumOfFrequencies() {
		return mSumOfFrequencies;
	}
}
