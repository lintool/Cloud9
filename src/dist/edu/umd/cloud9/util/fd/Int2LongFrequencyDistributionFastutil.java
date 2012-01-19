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

import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.longs.LongCollection;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Lists;

import edu.umd.cloud9.io.pair.PairOfIntLong;

/**
 * Implementation of {@link Int2LongFrequencyDistribution} based on {@link Int2LongOpenHashMap}.
 *
 * @author Jimmy Lin
 */
public class Int2LongFrequencyDistributionFastutil implements Int2LongFrequencyDistribution {
	private Int2LongOpenHashMap counts = new Int2LongOpenHashMap();
	private long sumOfCounts = 0;

	@Override
	public void increment(int key) {
		if (contains(key)) {
			set(key, get(key) + 1L);
		} else {
			set(key, 1L);
		}
	}

	@Override
	public void increment(int key, long cnt) {
		if (contains(key)) {
			set(key, get(key) + cnt);
		} else {
			set(key, cnt);
		}
	}

	@Override
	public void decrement(int key) {
		if (contains(key)) {
			long v = get(key);
			if (v == 1L) {
				remove(key);
			} else {
				set(key, v - 1L);
			}
		} else {
			throw new RuntimeException("Can't decrement non-existent event!");
		}
	}

	@Override
	public void decrement(int key, long cnt) {
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
	public boolean contains(int key) {
		return counts.containsKey(key);
	}

	@Override
	public long get(int key) {
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
	public long set(int k, long v) {
		long rv = counts.put(k, v);
		sumOfCounts = sumOfCounts - rv + v;

		return rv;
	}

	@Override
	public long remove(int k) {
		long rv = counts.remove(k);
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
	public LongCollection values() {
		return counts.values();
	}

	/**
	 * Exposes efficient method for accessing mappings in this map.
	 */
	public Int2LongMap.FastEntrySet entrySet() {
		return counts.int2LongEntrySet();
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
	public Iterator<PairOfIntLong> iterator() {
		return new Iterator<PairOfIntLong>() {
			private Iterator<Int2LongMap.Entry> iter =
			  Int2LongFrequencyDistributionFastutil.this.counts.int2LongEntrySet().iterator();
			private final PairOfIntLong pair = new PairOfIntLong();

			@Override
			public boolean hasNext() {
				return iter.hasNext();
			}

			@Override
			public PairOfIntLong next() {
				if (!hasNext()) {
					return null;
				}

				Int2LongMap.Entry entry = iter.next();
				pair.set(entry.getIntKey(), entry.getLongValue());
				return pair;
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}

  @Override
  public List<PairOfIntLong> getEntries(Order ordering) {
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
    return null;
  }

  @Override
  public List<PairOfIntLong> getEntries(Order ordering, int n) {
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

  private final Comparator<PairOfIntLong> comparatorRightDescending =
    new Comparator<PairOfIntLong>() {
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
    };

  private final Comparator<PairOfIntLong> comparatorRightAscending =
    new Comparator<PairOfIntLong>() {
      public int compare(PairOfIntLong e1, PairOfIntLong e2) {
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

  private final Comparator<PairOfIntLong> comparatorLeftAscending =
    new Comparator<PairOfIntLong>() {
      public int compare(PairOfIntLong e1, PairOfIntLong e2) {
        if (e1.getLeftElement() > e2.getLeftElement()) {
          return 1;
        }

        if (e1.getLeftElement() < e2.getLeftElement()) {
          return -1;
        }

        throw new RuntimeException("Event observed twice!");
      }
    };

  private final Comparator<PairOfIntLong> comparatorLeftDescending =
    new Comparator<PairOfIntLong>() {
      public int compare(PairOfIntLong e1, PairOfIntLong e2) {
        if (e1.getLeftElement() > e2.getLeftElement()) {
          return -1;
        }

        if (e1.getLeftElement() < e2.getLeftElement()) {
          return 1;
        }

        throw new RuntimeException("Event observed twice!");
      }
    };

  private List<PairOfIntLong> getEntriesSorted(Comparator<PairOfIntLong> comparator) {
    List<PairOfIntLong> list = Lists.newArrayList();

    for (Int2LongMap.Entry e : counts.int2LongEntrySet()) {
      list.add(new PairOfIntLong(e.getIntKey(), e.getLongValue()));
    }

    Collections.sort(list, comparator);
    return list;
  }

  private List<PairOfIntLong> getEntriesSorted(Comparator<PairOfIntLong> comparator, int n) {
    List<PairOfIntLong> list = getEntriesSorted(comparator);
    return list.subList(0, n);
  }
}
