package edu.umd.cloud9.util;

import java.util.Arrays;
import java.util.Comparator;

public class OHMapIF extends HMapIF {

	private static final long serialVersionUID = 823615346L;

	/**
	 * Treats maps as if they were vectors and performs vector addition.
	 * 
	 * @param m
	 *            the other vector
	 */
	public void plus(HMapIF m) {
		for (MapIF.Entry e : m.entrySet()) {
			int key = e.getKey();

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
	public float dot(HMapIF m) {
		float s = 0.0f;

		for (MapIF.Entry e : m.entrySet()) {
			int key = e.getKey();

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

		for (MapIF.Entry e : this.entrySet()) {
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

		for (int f : this.keySet()) {
			this.put(f, this.get(f) / l);
		}

	}

	/**
	 * Returns entries sorted by descending value. Ties broken by the natural
	 * sort order of the feature.
	 * 
	 * @return entries sorted by descending value
	 */
	public Entry[] getEntriesSortedByValue() {
		if (this.size() == 0)
			return null;

		// for storing the entries
		Entry[] entries = new Entry[this.size()];
		int i = 0;
		Entry next = null;

		int index = 0;
		// advance to first entry
		while (index < table.length && (next = table[index++]) == null)
			;

		while (next != null) {
			// current entry
			Entry e = next;

			// advance to next entry
			next = e.next;
			if ((next = e.next) == null) {
				while (index < table.length && (next = table[index++]) == null)
					;
			}

			// add entry to array
			entries[i++] = e;
		}

		// sort the entries
		Arrays.sort(entries, new Comparator<MapIF.Entry>() {
			@SuppressWarnings("unchecked")
			public int compare(MapIF.Entry e1, MapIF.Entry e2) {
				if (e1.getValue() > e2.getValue()) {
					return -1;
				} else if (e1.getValue() < e2.getValue()) {
					return 1;
				}

				if (e1.getKey() == e2.getKey())
					return 0;

				return e1.getKey() > e2.getKey() ? 1 : -1;
			}
		});

		return entries;
	}

	/**
	 * Returns top <i>n</i> entries sorted by descending value. Ties broken by
	 * the natural sort order of the feature.
	 * 
	 * @param n
	 *            number of entries to return
	 * @return top <i>n</i> entries sorted by descending value
	 */
	public Entry[] getEntriesSortedByValue(int n) {
		Entry[] entries = getEntriesSortedByValue();

		if (entries == null)
			return null;

		if (entries.length < n)
			return entries;

		// return Arrays.copyOfRange(entries, 0, n);

		// copyOfRange isn't available until Java 1.6, so it doesn't run on the
		// Google/IBM cluster.
		Entry[] r = new Entry[n];
		for (int i = 0; i < n; i++) {
			r[i] = entries[i];
		}

		return r;
	}

}
