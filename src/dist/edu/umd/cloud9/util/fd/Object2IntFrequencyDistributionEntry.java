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
import java.util.Set;

import com.google.common.collect.Lists;

import edu.umd.cloud9.util.map.HMapKI;
import edu.umd.cloud9.util.map.MapKI;
import edu.umd.cloud9.util.pair.PairOfObjectInt;

/**
 * Implementation of {@link Object2IntFrequencyDistribution} based on {@link HMapKI}.
 *
 * @author Jimmy Lin
 */
public class Object2IntFrequencyDistributionEntry<K extends Comparable<K>>
    implements Object2IntFrequencyDistribution<K> {

	private MapKI<K> counts = new HMapKI<K>();
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
		return counts.get(k);
	}

	@Override
	public int set(K k, int v) {
		int rv = counts.put(k, v);
		sumOfCounts = sumOfCounts - rv + v;

		return rv;
	}

  @Override
  public double computeRelativeFrequency(K k) {
    return (double) counts.get(k) / getSumOfCounts();
  }

  @Override
  public double computeLogRelativeFrequency(K k) {
    return Math.log(counts.get(k)) - Math.log(getSumOfCounts());
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


	@Override
	public int getNumberOfEvents() {
		return counts.size();
	}

	@Override
	public long getSumOfCounts() {
		return sumOfCounts;
	}

	@Override
  public Set<K> keySet() {
	  return counts.keySet();
	}

	/**
	 * Iterator returns the same object every time, just with a different payload.
	 */
	public Iterator<PairOfObjectInt<K>> iterator() {
		return new Iterator<PairOfObjectInt<K>>() {
			private Iterator<MapKI.Entry<K>> iter =
			  Object2IntFrequencyDistributionEntry.this.counts.entrySet().iterator();
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

				MapKI.Entry<K> entry = iter.next();
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
  public List<PairOfObjectInt<K>> getEntries(Order ordering) {
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
  public List<PairOfObjectInt<K>> getEntries(Order ordering, int n) {
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

  private final Comparator<PairOfObjectInt<K>> comparatorRightDescending =
    new Comparator<PairOfObjectInt<K>>() {
      public int compare(PairOfObjectInt<K> e1, PairOfObjectInt<K> e2) {
        if (e1.getRightElement() > e2.getRightElement()) {
          return -1;
        }

        if (e1.getRightElement() < e2.getRightElement()) {
          return 1;
        }

        return e1.getLeftElement().compareTo(e2.getLeftElement());
      }
    };

  private final Comparator<PairOfObjectInt<K>> comparatorRightAscending =
    new Comparator<PairOfObjectInt<K>>() {
      public int compare(PairOfObjectInt<K> e1, PairOfObjectInt<K> e2) {
        if (e1.getRightElement() > e2.getRightElement()) {
          return 1;
        }

        if (e1.getRightElement() < e2.getRightElement()) {
          return -1;
        }

        return e1.getLeftElement().compareTo(e2.getLeftElement());
      }
    };

  private final Comparator<PairOfObjectInt<K>> comparatorLeftAscending =
    new Comparator<PairOfObjectInt<K>>() {
      public int compare(PairOfObjectInt<K> e1, PairOfObjectInt<K> e2) {
        if (e1.getLeftElement().equals(e2.getLeftElement())) {
          throw new RuntimeException("Event observed twice!");
        }

        return e1.getLeftElement().compareTo(e2.getLeftElement());
      }
    };

  private final Comparator<PairOfObjectInt<K>> comparatorLeftDescending =
    new Comparator<PairOfObjectInt<K>>() {
      public int compare(PairOfObjectInt<K> e1, PairOfObjectInt<K> e2) {
        if (e1.getLeftElement().equals(e2.getLeftElement())) {
          throw new RuntimeException("Event observed twice!");
        }

        return e2.getLeftElement().compareTo(e1.getLeftElement());
      }
    };

  private List<PairOfObjectInt<K>> getEntriesSorted(Comparator<PairOfObjectInt<K>> comparator) {
    List<PairOfObjectInt<K>> list = Lists.newArrayList();

    for (MapKI.Entry<K> e : counts.entrySet()) {
      list.add(new PairOfObjectInt<K>(e.getKey(), e.getValue()));
    }

    Collections.sort(list, comparator);
    return list;
  }

  private List<PairOfObjectInt<K>>
      getEntriesSorted(Comparator<PairOfObjectInt<K>> comparator, int n) {
    List<PairOfObjectInt<K>> list = getEntriesSorted(comparator);
    return list.subList(0, n);
  }
}
