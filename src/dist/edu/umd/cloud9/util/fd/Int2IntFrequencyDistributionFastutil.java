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

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntCollection;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Lists;

import edu.umd.cloud9.io.pair.PairOfInts;

/**
 * Implementation of {@link Int2IntFrequencyDistribution} based on
 * {@link Int2IntOpenHashMap}.
 *
 * @author Jimmy Lin
 */
public class Int2IntFrequencyDistributionFastutil implements Int2IntFrequencyDistribution {
	private Int2IntOpenHashMap counts = new Int2IntOpenHashMap();
	private long sumOfCounts = 0;

	@Override
	public void increment(int key) {
		if (contains(key)) {
			set(key, get(key) + 1);
		} else {
			set(key, 1);
		}
	}

	@Override
	public void increment(int key, int cnt) {
		if (contains(key)) {
			set(key, get(key) + cnt);
		} else {
			set(key, cnt);
		}
	}

	@Override
	public void decrement(int key) {
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
	public void decrement(int key, int cnt) {
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
	public boolean contains(int key) {
		return counts.containsKey(key);
	}

	@Override
	public int get(int key) {
		return counts.get(key);
	}

  @Override
  public double computeRelativeFrequency(int k) {
    return (double) counts.get(k) / getSumOfCounts();
  }

  @Override
  public double computeLogRelativeFrequency(int k) {
    return Math.log(counts.get(k)) - Math.log(getSumOfCounts());
  }

	@Override
	public int set(int key, int cnt) {
		int rv = counts.put(key, cnt);
		sumOfCounts = sumOfCounts - rv + cnt;

		return rv;
	}

	@Override
	public int remove(int key) {
		int rv = counts.remove(key);
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
	public IntSet keySet() {
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
	public Int2IntMap.FastEntrySet entrySet() {
		return counts.int2IntEntrySet();
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
	public Iterator<PairOfInts> iterator() {
		return new Iterator<PairOfInts>() {
			private Iterator<Int2IntMap.Entry> iter = Int2IntFrequencyDistributionFastutil.this.counts.int2IntEntrySet().iterator();
			private final PairOfInts pair = new PairOfInts();

			@Override
			public boolean hasNext() {
				return iter.hasNext();
			}

			@Override
			public PairOfInts next() {
				if (!hasNext()) {
					return null;
				}

				Int2IntMap.Entry entry = iter.next();
				pair.set(entry.getIntKey(), entry.getIntValue());
				return pair;
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}

  @Override
  public List<PairOfInts> getEntries(Order ordering) {
    if (ordering.equals(Order.ByRightElementDescending)) {
      return getEntriesSorted(comparatorRightDescending);
    } else if (ordering.equals(Order.ByLeftElementAscending)) {
      return getEntriesSorted(comparatorLeftAscending);
    } else if (ordering.equals(Order.ByRightElementAscending)) {
      return getEntriesSorted(comparatorRightAscending);
    } else if (ordering.equals(Order.ByLeftElementDescending)) {
      return getEntriesSorted(comparatorLeftDescending);
    }
    // Should never get here.
    return null;  }

  @Override
  public List<PairOfInts> getEntries(Order ordering, int n) {
    if (ordering.equals(Order.ByRightElementDescending)) {
      return getEntriesSorted(comparatorRightDescending, n);
    } else if (ordering.equals(Order.ByLeftElementAscending)) {
      return getEntriesSorted(comparatorLeftAscending, n);
    } else if (ordering.equals(Order.ByRightElementAscending)) {
      return getEntriesSorted(comparatorRightAscending, n);
    } else if (ordering.equals(Order.ByLeftElementDescending)) {
      return getEntriesSorted(comparatorLeftDescending, n);
    }
    // Should never get here.
    return null;
  }

  private final Comparator<PairOfInts> comparatorRightDescending = new Comparator<PairOfInts>() {
    public int compare(PairOfInts e1, PairOfInts e2) {
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
  };

  private final Comparator<PairOfInts> comparatorRightAscending = new Comparator<PairOfInts>() {
    public int compare(PairOfInts e1, PairOfInts e2) {
      if (e1.getRightElement() > e2.getRightElement()) {
        return 1;
      }

      if (e1.getRightElement() < e2.getRightElement()) {
        return -1;
      }

      if (e1.getLeftElement() == e2.getLeftElement()) {
        throw new RuntimeException("Event observed twice!");
      }

      return e1.getLeftElement() < e2.getLeftElement() ? -1 : 1;
    }
  };

  private final Comparator<PairOfInts> comparatorLeftAscending = new Comparator<PairOfInts>() {
    public int compare(PairOfInts e1, PairOfInts e2) {
      if (e1.getLeftElement() > e2.getLeftElement()) {
        return 1;
      }

      if (e1.getLeftElement() < e2.getLeftElement()) {
        return -1;
      }

      throw new RuntimeException("Event observed twice!");
    }
  };

  private final Comparator<PairOfInts> comparatorLeftDescending = new Comparator<PairOfInts>() {
    public int compare(PairOfInts e1, PairOfInts e2) {
      if (e1.getLeftElement() > e2.getLeftElement()) {
        return -1;
      }

      if (e1.getLeftElement() < e2.getLeftElement()) {
        return 1;
      }

      throw new RuntimeException("Event observed twice!");
    }
  };

  private List<PairOfInts> getEntriesSorted(Comparator<PairOfInts> comparator) {
    List<PairOfInts> list = Lists.newArrayList();

    for (Int2IntMap.Entry e : counts.int2IntEntrySet()) {
      list.add(new PairOfInts(e.getIntKey(), e.getIntValue()));
    }

    Collections.sort(list, comparator);
    return list;
  }

  private List<PairOfInts> getEntriesSorted(Comparator<PairOfInts> comparator, int n) {
    List<PairOfInts> list = getEntriesSorted(comparator);
    return list.subList(0, n);
  }
}
