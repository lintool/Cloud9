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

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Lists;

import edu.umd.cloud9.io.pair.PairOfInts;
import edu.umd.cloud9.util.map.HMapII;
import edu.umd.cloud9.util.map.MapII;

/**
 * Implementation of {@link Int2IntFrequencyDistribution} based on {@link HMapII}.
 *
 * @author Jimmy Lin
 */
public class Int2IntFrequencyDistributionEntry implements Int2IntFrequencyDistribution {
	private HMapII counts = new HMapII();
	private long sumOfFrequencies = 0;

	@Override
	public void increment(int key) {
		set(key, get(key) + 1);
	}

	@Override
	public void increment(int key, int cnt) {
		set(key, get(key) + cnt);
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
		sumOfFrequencies = sumOfFrequencies - rv + cnt;

		return rv;
	}

	@Override
	public int remove(int key) {
		int rv = counts.remove(key);
		sumOfFrequencies -= rv;

		return rv;
	}

	@Override
	public void clear() {
		counts.clear();
		sumOfFrequencies = 0;
	}

	@Override
	public int getNumberOfEvents() {
		return counts.size();
	}

	@Override
	public long getSumOfCounts() {
		return sumOfFrequencies;
	}

	/**
	 * Iterator returns the same object every time, just with a different payload.
	 */
	public Iterator<PairOfInts> iterator() {
		return new Iterator<PairOfInts>() {
			private Iterator<MapII.Entry> iter = Int2IntFrequencyDistributionEntry.this.counts.entrySet().iterator();
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

				MapII.Entry entry = iter.next();
				pair.set(entry.getKey(), entry.getValue());
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
    return null;
  }

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

    for (MapII.Entry e : counts.entrySet()) {
      list.add(new PairOfInts(e.getKey(), e.getValue()));
    }

    Collections.sort(list, comparator);
    return list;
  }

  private List<PairOfInts> getEntriesSorted(Comparator<PairOfInts> comparator, int n) {
    List<PairOfInts> list = getEntriesSorted(comparator);
    return list.subList(0, n);
  }
}
