package edu.umd.cloud9.util;

import java.util.Collections;
import java.util.Comparator;
import java.util.SortedSet;
import java.util.TreeSet;

public class OrderedHashMapFloat<K extends Comparable> extends HashMapFloat<K> {

	private static final long serialVersionUID = 6590482318L;

	/**
	 * Treats maps as if they were vectors and performs vector addition.
	 * 
	 * @param m
	 *            the other vector
	 */
	public void plus(OrderedHashMapFloat<K> m) {
		for (MapFloat.Entry<K> e : m.entrySet()) {
			K key = e.getKey();

			if (this.containsKey(key)) {
				this.put(key, this.get(key) + e.getValue());
			} else {
				this.put(key, e.getValue());
			}
		}
	}

	/**
	 * Treats maps as if they were vectors and computes the dot product.
	 * 
	 * @param m
	 *            the other vector
	 */
	public float dot(OrderedHashMapFloat<K> m) {
		float s = 0.0f;

		for (MapFloat.Entry<K> e : m.entrySet()) {
			K key = e.getKey();

			if (this.containsKey(key)) {
				s += this.get(key) * e.getValue();
			}
		}

		return s;
	}

	/**
	 * Treats this map as if it were a vector and returns its length.
	 * 
	 * @return length of this vector
	 */
	public float length() {
		float s = 0.0f;

		for (MapFloat.Entry<K> e : this.entrySet()) {
			s += e.getValue() * e.getValue();
		}

		return (float) Math.sqrt(s);
	}

	/**
	 * Treats this map as if it were a vector and normalizes it to a unit-length
	 * vector.
	 */
	public void normalize() {
		float l = this.length();

		for (K f : this.keySet()) {
			this.put(f, this.get(f) / l);
		}

	}

	/**
	 * Returns entries sorted by descending value. Ties broken by the natural
	 * sort order of the feature.
	 * 
	 * @return entries sorted by descending value
	 */
	public SortedSet<MapFloat.Entry<K>> getEntriesSortedByValue() {
		SortedSet<MapFloat.Entry<K>> entries = new TreeSet<MapFloat.Entry<K>>(
				new Comparator<MapFloat.Entry<K>>() {
					@SuppressWarnings("unchecked")
					public int compare(MapFloat.Entry<K> e1, MapFloat.Entry<K> e2) {
						if (e1.getValue() > e2.getValue()) {
							return -1;
						} else if (e1.getValue() < e2.getValue()) {
							return 1;
						}
						return e1.getKey().compareTo(e2.getKey());
					}
				});

		for (MapFloat.Entry<K> entry : this.entrySet()) {
			entries.add(entry);
		}

		return Collections.unmodifiableSortedSet(entries);
	}

	/**
	 * Returns top <i>n</i> entries sorted by descending value. Ties broken by
	 * the natural sort order of the feature.
	 * 
	 * @param n
	 *            number of entries to return
	 * @return top <i>n</i> entries sorted by descending value
	 */
	public SortedSet<MapFloat.Entry<K>> getEntriesSortedByValue(int n) {
		// TODO: this should be rewritten to use a Fibonacci heap
		
		SortedSet<MapFloat.Entry<K>> entries = new TreeSet<MapFloat.Entry<K>>(
				new Comparator<MapFloat.Entry<K>>() {
					@SuppressWarnings("unchecked")
					public int compare(MapFloat.Entry<K> e1, MapFloat.Entry<K> e2) {
						if (e1.getValue() > e2.getValue()) {
							return -1;
						} else if (e1.getValue() < e2.getValue()) {
							return 1;
						}
						return e1.getKey().compareTo(e2.getKey());
					}
				});

		int cnt = 0;
		for (MapFloat.Entry<K> entry : getEntriesSortedByValue()) {
			entries.add(entry);
			cnt++;
			if (cnt >= n)
				break;
		}

		return Collections.unmodifiableSortedSet(entries);
	}
}
