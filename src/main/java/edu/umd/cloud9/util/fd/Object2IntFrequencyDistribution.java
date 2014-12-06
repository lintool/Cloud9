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

import java.util.Set;

import edu.umd.cloud9.util.SortableEntries;
import edu.umd.cloud9.util.pair.PairOfObjectInt;

/**
 * A frequency distribution where events are arbitrary objects and counts are ints.
 *
 * @author Jimmy Lin
 */
public interface Object2IntFrequencyDistribution<K extends Comparable<K>> extends
    SortableEntries<PairOfObjectInt<K>> {

  /**
   * Increments the count of an event {@code key}.
   */
  public void increment(K key);

  /**
   * Increments the count of an event {@code key} by {@code cnt}.
   */
  public void increment(K key, int cnt);

  /**
   * Decrements the count of an event {@code key}.
   */
  public void decrement(K key);

  /**
   * Decrements the count of a particular event {@code key} by {@code cnt}.
   */
  public void decrement(K key, int cnt);

  /**
   * Returns {@code true} if {@code key} exists in this object.
   */
  public boolean contains(K key);

  /**
   * Returns the count of a particular event {@code key}.
   */
  public int get(K key);

  /**
   * Computes the relative frequency of a particular event {@code key}.
   * That is, {@code f(key) / SUM_i f(key_i)}.
   */
  public double computeRelativeFrequency(K key);

  /**
   * Computes the log (base e) of the relative frequency of a particular event {@code key}.
   */
  public double computeLogRelativeFrequency(K key);

  /**
   * Sets the count of a particular event {@code key} to {@code cnt}.
   */
  public int set(K key, int cnt);

  /**
   * Removes the count of a particular event {@code key}.
   */
  public int remove(K k);

  /**
   * Removes all events.
   */
  public void clear();

  /**
   * Returns number of distinct events observed. Note that if an event is observed and then its
   * count subsequently removed, the event will not be included in this count.
   */
  public int getNumberOfEvents();

  /**
   * Returns the sum of counts of all observed events. That is, {@code SUM_i f(key_i)}.
   */
  public long getSumOfCounts();

  /**
   * Returns the set of keys.
   */
  public Set<K> keySet();
}
