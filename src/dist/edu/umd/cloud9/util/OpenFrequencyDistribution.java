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

/**
 * An implementation of a frequency distribution for arbitrary events, backed by
 * a fastutil open hash map. One possible use is to store frequency counts for
 * language models, where the events are terms. This class keeps track of
 * frequencies using ints, so beware when dealing with a large number of
 * observations; see also {@link OpenLargeFrequencyDistribution}.
 *
 * @author Jimmy Lin
 *
 */
public class OpenFrequencyDistribution<K extends Comparable<K>> implements FrequencyDistribution<K> {

	private Object2IntOpenHashMap<K> mCounts = new Object2IntOpenHashMap<K>();
	private long mSumOfFrequencies = 0;

	@Override
	public void increment(K key) {
		if (contains(key)) {
			set(key, get(key) + 1);
		} else {
			set(key, 1);
		}
	}

	@Override
	public void increment(K key, int cnt) {
		if (contains(key)) {
			set(key, get(key) + cnt);
		} else {
			set(key, cnt);
		}
	}

	@Override
	public void decrement(K key) {
		if (contains(key)) {
			int v = get(key);
			if (v == 1) {
				remove(key);
			} else {
				set(key, v - 1);
			}
		} else {
			throw new RuntimeException("Can't decrement non-existent event!");
		}
	}

	@Override
	public void decrement(K key, int cnt) {
		if (contains(key)) {
			int v = get(key);
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
	public boolean contains(K k) {
		return mCounts.containsKey(k);
	}

	@Override
	public int get(K k) {
		return mCounts.getInt(k);
	}

	@Override
	public int set(K k, int v) {
		int rv = mCounts.put(k, v);
		mSumOfFrequencies = mSumOfFrequencies - rv + v;

		return rv;
	}

	@Override
	public int remove(K k) {
		int rv = mCounts.remove(k);
		mSumOfFrequencies -= rv;

		return rv;
	}

	@Override
	public List<PairOfObjectInt<K>> getFrequencySortedEvents() {
		List<PairOfObjectInt<K>> list = Lists.newArrayList();

		for (Object2IntMap.Entry<K> e : mCounts.object2IntEntrySet()) {
			list.add(new PairOfObjectInt<K>(e.getKey(), e.getIntValue()));
		}

		Collections.sort(list, new Comparator<PairOfObjectInt<K>>() {
			public int compare(PairOfObjectInt<K> e1, PairOfObjectInt<K> e2) {
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
	public List<PairOfObjectInt<K>> getFrequencySortedEvents(int n) {
		List<PairOfObjectInt<K>> list = getFrequencySortedEvents();
		return list.subList(0, n);
	}

	@Override
	public List<PairOfObjectInt<K>> getSortedEvents() {
		List<PairOfObjectInt<K>> list = Lists.newArrayList();

		for (Object2IntMap.Entry<K> e : mCounts.object2IntEntrySet()) {
			list.add(new PairOfObjectInt<K>(e.getKey(), e.getIntValue()));
		}

		// sort the entries
		Collections.sort(list, new Comparator<PairOfObjectInt<K>>() {
			public int compare(PairOfObjectInt<K> e1, PairOfObjectInt<K> e2) {
				if (e1.getLeftElement().equals(e2.getLeftElement())) {
					throw new RuntimeException("Event observed twice!");
				}

				return e1.getLeftElement().compareTo(e1.getLeftElement());
			}
		});

		return list;
	}

	@Override
	public List<PairOfObjectInt<K>> getSortedEvents(int n) {
		List<PairOfObjectInt<K>> list = getSortedEvents();
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
