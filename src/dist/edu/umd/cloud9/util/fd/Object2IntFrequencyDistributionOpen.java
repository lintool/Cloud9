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

package edu.umd.cloud9.util.fd;

import it.unimi.dsi.fastutil.ints.IntCollection;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectSet;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Lists;

import edu.umd.cloud9.util.pair.PairOfObjectInt;

/**
 * Implementation of {@link Object2IntFrequencyDistribution} based on
 * {@link Object2IntOpenHashMap}.
 *
 * @author Jimmy Lin
 *
 */
public class Object2IntFrequencyDistributionOpen<K extends Comparable<K>>
    implements Object2IntFrequencyDistribution<K> {

	private Object2IntOpenHashMap<K> counts = new Object2IntOpenHashMap<K>();
	private long sumOfCounts = 0;

	@Override
	public void increment(K key) {
		set(key, get(key) + 1);
	}

	@Override
	public void increment(K key, int cnt) {
		set(key, get(key) + cnt);
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
		return counts.containsKey(k);
	}

	@Override
	public int get(K k) {
		return counts.getInt(k);
	}

  @Override
  public float getFrequency(K k) {
    return (float) counts.getInt(k) / getSumOfCounts();
  }

  @Override
  public float getLogFrequency(K k) {
    return (float) (Math.log(counts.getInt(k)) - Math.log(getSumOfCounts()));
  }

	@Override
	public int set(K k, int v) {
		int rv = counts.put(k, v);
		sumOfCounts = sumOfCounts - rv + v;

		return rv;
	}

	@Override
	public int remove(K k) {
		int rv = counts.remove(k);
		sumOfCounts -= rv;

		return rv;
	}

	@Override
	public void clear() {
		counts.clear();
		sumOfCounts = 0;
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
	public IntCollection values() {
		return counts.values();
	}

	/**
	 * Exposes efficient method for accessing mappings in this map.
	 */
	public Object2IntMap.FastEntrySet<K> entrySet() {
		return counts.object2IntEntrySet();
	}

  /**
   * Returns the underlying map backing this frequency distribution. Manipulate
   * this map at your own risk; may render this frequency distribution unusable.
   */
	public Object2IntOpenHashMap<K> getObject2IntOpenHashMap() {
	  return counts;
	}

	@Override
	public int getNumberOfEvents() {
		return counts.size();
	}

	@Override
	public long getSumOfCounts() {
		return sumOfCounts;
	}

	/**
	 * Iterator returns the same object every time, just with a different payload.
	 */
	public Iterator<PairOfObjectInt<K>> iterator() {
		return new Iterator<PairOfObjectInt<K>>() {
			private Iterator<Object2IntMap.Entry<K>> iter =
			  Object2IntFrequencyDistributionOpen.this.counts.object2IntEntrySet().iterator();
			private final PairOfObjectInt<K> pair = new PairOfObjectInt<K>();

			@Override
			public boolean hasNext() {
				return iter.hasNext();
			}

			@Override
			public PairOfObjectInt<K> next() {
				if (!hasNext()) {
					return null;
				}

				Object2IntMap.Entry<K> entry = iter.next();
				pair.set(entry.getKey(), entry.getIntValue());
				return pair;
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}

  @Override
  public List<PairOfObjectInt<K>> getEntries(Order ordering) {
    if (ordering.equals(Order.ByLeftElementDescending)) {
      return getSortedEvents();
    } else if (ordering.equals(Order.ByRightElementDescending)) {
      return getEventsSortedByCount();
    }

    // TODO: Implement other sort orders.
    throw new UnsupportedOperationException();
  }

  @Override
  public List<PairOfObjectInt<K>> getEntries(Order ordering, int n) {
    if (ordering.equals(Order.ByLeftElementDescending)) {
      return getSortedEvents(n);
    } else if (ordering.equals(Order.ByRightElementDescending)) {
      return getEventsSortedByCount(n);
    }

    // TODO: Implement other sort orders.
    throw new UnsupportedOperationException();
  }

  private List<PairOfObjectInt<K>> getEventsSortedByCount() {
    List<PairOfObjectInt<K>> list = Lists.newArrayList();

    for (Object2IntMap.Entry<K> e : counts.object2IntEntrySet()) {
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

  private List<PairOfObjectInt<K>> getEventsSortedByCount(int n) {
    List<PairOfObjectInt<K>> list = getEventsSortedByCount();
    return list.subList(0, n);
  }

  private List<PairOfObjectInt<K>> getSortedEvents() {
    List<PairOfObjectInt<K>> list = Lists.newArrayList();

    for (Object2IntMap.Entry<K> e : counts.object2IntEntrySet()) {
      list.add(new PairOfObjectInt<K>(e.getKey(), e.getIntValue()));
    }

    // sort the entries
    Collections.sort(list, new Comparator<PairOfObjectInt<K>>() {
      public int compare(PairOfObjectInt<K> e1, PairOfObjectInt<K> e2) {
        if (e1.getLeftElement().equals(e2.getLeftElement())) {
          throw new RuntimeException("Event observed twice!");
        }

        return e1.getLeftElement().compareTo(e2.getLeftElement());
      }
    });

    return list;
  }

  private List<PairOfObjectInt<K>> getSortedEvents(int n) {
    List<PairOfObjectInt<K>> list = getSortedEvents();
    return list.subList(0, n);
  }

  public static <T extends Comparable<T>> Object2IntFrequencyDistributionOpen<T>
      fromObject2IntOpenHashMap(Object2IntOpenHashMap<T> map) {
    Object2IntFrequencyDistributionOpen<T> fd = new Object2IntFrequencyDistributionOpen<T>();

    fd.counts = map;
    long cnt = 0;
    for ( Object2IntMap.Entry<T> entry : map.object2IntEntrySet() ) {
      cnt += entry.getIntValue();
    }
    fd.sumOfCounts = cnt;

    return fd;
  }
}
