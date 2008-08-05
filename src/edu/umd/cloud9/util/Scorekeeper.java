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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * An object that holds scores associated with each object (the key) and
 * supports iteration by score (descending). Many applications call for this
 * type of functionality, e.g., keeping track of the score of each document in
 * an document retrieval application.
 * 
 * @param <K>
 *            type of key
 */
public class Scorekeeper<K extends Comparable<K>, V extends Number & Comparable<V>> extends
		HashMap<K, V> {

	private static final long serialVersionUID = 2983410765L;

	/**
	 * Constructs a <code>ScoreSortedMap</code>.
	 */
	public Scorekeeper() {
		super();
	}

	/**
	 * Returns the all entries sorted by scores.
	 * 
	 * @return a sorted set view of the entries sorted by scores
	 */
	public SortedSet<Map.Entry<K, V>> getSortedEntries() {
		SortedSet<Map.Entry<K, V>> entries = new TreeSet<Map.Entry<K, V>>(
				new Comparator<Map.Entry<K, V>>() {
					public int compare(Map.Entry<K, V> e1, Map.Entry<K, V> e2) {

						if (e2.getValue().compareTo(e1.getValue()) == 0) {
							return e2.getKey().compareTo(e1.getKey());
						}

						return e2.getValue().compareTo(e1.getValue());
					}
				});

		for (Map.Entry<K, V> entry : this.entrySet()) {
			entries.add(entry);
		}

		return Collections.unmodifiableSortedSet(entries);
	}

	/**
	 * Returns the <i>n</i> top entries sorted by scores.
	 * 
	 * @param n
	 *            number of entries to retrieve
	 * @return a Set view of the entries sorted by scores
	 */
	public SortedSet<Map.Entry<K, V>> getSortedEntries(int n) {
		SortedSet<Map.Entry<K, V>> entries = new TreeSet<Map.Entry<K, V>>(
				new Comparator<Map.Entry<K, V>>() {
					public int compare(Map.Entry<K, V> e1, Map.Entry<K, V> e2) {

						if (e2.getValue().compareTo(e1.getValue()) == 0) {
							return e2.getKey().compareTo(e1.getKey());
						}

						return e2.getValue().compareTo(e1.getValue());
					}
				});

		int cnt = 0;
		for (Map.Entry<K, V> entry : getSortedEntries()) {
			entries.add(entry);
			cnt++;
			if (cnt >= n)
				break;
		}

		return Collections.unmodifiableSortedSet(entries);
	}

	/**
	 * Returns the top-scoring entry.
	 * 
	 * @return the top-scoring entry
	 */
	public Map.Entry<K, V> getTopEntry() {
		return getSortedEntries().first();
	}

	/**
	 * Returns the <i>i</i>th scoring entry.
	 * 
	 * @param i
	 *            the rank
	 * @return the <i>i</i>th scoring entry
	 */
	public Map.Entry<K, V> getEntryByRank(int i) {
		if (i > this.size())
			throw new NoSuchElementException("Error: index out of bounds");

		Iterator<Map.Entry<K, V>> iter = getSortedEntries().iterator();

		int n = 0;
		while (n++ < i - 1)
			iter.next();

		return iter.next();
	}

	/**
	 * Returns a list of the keys, sorted by score.
	 * 
	 * @return a list of the keys, sorted by score
	 */
	public List<K> getSortedKeys() {
		List<K> list = new ArrayList<K>();

		for (Map.Entry<K, V> entry : getSortedEntries()) {
			list.add(entry.getKey());
		}

		return list;
	}
}
