package edu.umd.cloud9.util;

import java.util.Collections;
import java.util.Comparator;
import java.util.SortedSet;
import java.util.TreeSet;

public class OrderedHashMapInt<K extends Comparable> extends HashMapInt<K> {

	private static final long serialVersionUID = 8726031451L;

	/**
	 * Adds another vector to this vector, based on feature-wise addition.
	 * 
	 * @param m
	 *            vector to add
	 */
	public void plus(OrderedHashMapInt<K> m) {
		for (MapInt.Entry<K> e : m.entrySet()) {
			K key = e.getKey();

			if (this.containsKey(key)) {
				this.put(key, this.get(key) + e.getValue());
			} else {
				this.put(key, e.getValue());
			}
		}
	}

	/**
	 * Computes the dot product between this vector and another vector.
	 * 
	 * @param m
	 *            the other vector
	 */
	public int dot(OrderedHashMapInt<K> m) {
		int s = 0;

		for (MapInt.Entry<K> e : m.entrySet()) {
			K key = e.getKey();

			if (this.containsKey(key)) {
				s += this.get(key) * e.getValue();
			}
		}

		return s;
	}

	/**
	 * Returns feature-value entries sorted by descending value. Ties broken by
	 * the natural sort order of the feature.
	 * 
	 * @return feature-value entries sorted by descending value
	 */
	public SortedSet<MapInt.Entry<K>> getEntriesSortedByValue() {
		SortedSet<MapInt.Entry<K>> entries = new TreeSet<MapInt.Entry<K>>(
				new Comparator<MapInt.Entry<K>>() {
					@SuppressWarnings("unchecked")
					public int compare(MapInt.Entry<K> e1, MapInt.Entry<K> e2) {
						if (e1.getValue() > e2.getValue()) {
							return -1;
						} else if (e1.getValue() < e2.getValue()) {
							return 1;
						}
						return e1.getKey().compareTo(e2.getKey());
					}
				});

		for (MapInt.Entry<K> entry : this.entrySet()) {
			entries.add(entry);
		}

		return Collections.unmodifiableSortedSet(entries);
	}

	/**
	 * Returns top <i>n</i> feature-value entries sorted by descending value.
	 * Ties broken by the natural sort order of the feature.
	 * 
	 * @param n
	 *            number of entries to return
	 * @return top <i>n</i> feature-value entries sorted by descending value
	 */
	public SortedSet<MapInt.Entry<K>> getEntriesSortedByValue(int n) {
		SortedSet<MapInt.Entry<K>> entries = new TreeSet<MapInt.Entry<K>>(
				new Comparator<MapInt.Entry<K>>() {
					@SuppressWarnings("unchecked")
					public int compare(MapInt.Entry<K> e1, MapInt.Entry<K> e2) {
						if (e1.getValue() > e2.getValue()) {
							return -1;
						} else if (e1.getValue() < e2.getValue()) {
							return 1;
						}
						return e1.getKey().compareTo(e2.getKey());
					}
				});

		int cnt = 0;
		for (MapInt.Entry<K> entry : getEntriesSortedByValue()) {
			entries.add(entry);
			cnt++;
			if (cnt >= n)
				break;
		}

		return Collections.unmodifiableSortedSet(entries);
	}
	
}
