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

package edu.umd.cloud9.util.count;

import it.unimi.dsi.fastutil.longs.LongCollection;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectSet;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Lists;

import edu.umd.cloud9.util.PairOfObjectLong;

/**
 * Implementation of {@link Object2LongFrequencyDistribution} based on
 * {@link Object2LongOpenHashMap}.
 *
 * @author Jimmy Lin
 *
 */
public class OpenObject2LongFrequencyDistribution<K extends Comparable<K>> implements Object2LongFrequencyDistribution<K> {

	private Object2LongOpenHashMap<K> counts = new Object2LongOpenHashMap<K>();
	private long sumOfFrequencies = 0;

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
		return counts.containsKey(key);
	}

	@Override
	public long get(K key) {
		return counts.getLong(key);
	}

	@Override
	public long set(K k, long v) {
		long rv = counts.put(k, v);
		sumOfFrequencies = sumOfFrequencies - rv + v;

		return rv;
	}

	@Override
	public long remove(K k) {
		long rv = counts.remove(k);
		sumOfFrequencies -= rv;

		return rv;
	}

	@Override
	public void clear() {
		counts.clear();
		sumOfFrequencies = 0;
	}

	/**
	 * Exposes efficient method for accessing keys in this map.
	 */
	public ObjectSet<K> keySet() {
		return counts.keySet();
	}

	/**
	 * Exposes efficient method for accessing values in this map.
	 */
	public LongCollection values() {
		return counts.values();
	}

	/**
	 * Exposes efficient method for accessing mappings in this map.
	 */
	public Object2LongMap.FastEntrySet<K> entrySet() {
		return counts.object2LongEntrySet();
	}

	@Override
	public List<PairOfObjectLong<K>> getFrequencySortedEvents() {
		List<PairOfObjectLong<K>> list = Lists.newArrayList();

		for (Object2LongMap.Entry<K> e : counts.object2LongEntrySet()) {
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

		for (Object2LongMap.Entry<K> e : counts.object2LongEntrySet()) {
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
		return counts.size();
	}

	@Override
	public long getSumOfFrequencies() {
		return sumOfFrequencies;
	}

	/**
	 * Iterator returns the same object every time, just with a different payload.
	 */
	public Iterator<PairOfObjectLong<K>> iterator() {
		return new Iterator<PairOfObjectLong<K>>() {
			private Iterator<Object2LongMap.Entry<K>> iter = OpenObject2LongFrequencyDistribution.this.counts.object2LongEntrySet().iterator();
			private final PairOfObjectLong<K> pair = new PairOfObjectLong<K>();

			@Override
			public boolean hasNext() {
				return iter.hasNext();
			}

			@Override
			public PairOfObjectLong<K> next() {
				if (!hasNext()) {
					return null;
				}

				Object2LongMap.Entry<K> entry = iter.next();
				pair.set(entry.getKey(), entry.getLongValue());
				return pair;
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}
}
