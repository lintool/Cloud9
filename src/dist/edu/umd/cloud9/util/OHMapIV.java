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

import java.util.Arrays;
import java.util.Comparator;

/**
 * Subclass of <code>HMapIV</code> that provides access to entries sorted by
 * value and other convenience methods.
 */
public class OHMapIV<V extends Comparable<?>> extends HMapIV<V> {

	private static final long serialVersionUID = 8605467218L;

	/**
	 * Returns entries sorted by descending value. Ties broken by the key.
	 * 
	 * @return entries sorted by descending value
	 */
	@SuppressWarnings("unchecked")
	public Entry<V>[] getEntriesSortedByValue() {
		if (this.size() == 0)
			return null;

		// for storing the entries
		Entry<V>[] entries = new Entry[this.size()];
		int i = 0;
		Entry<V> next = null;

		int index = 0;
		// advance to first entry
		while (index < table.length && (next = table[index++]) == null)
			;

		while (next != null) {
			// current entry
			Entry<V> e = next;

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
		Arrays.sort(entries, new Comparator<Entry<V>>() {
			@SuppressWarnings("unchecked")
			public int compare(Entry e1, Entry e2) {
				return ((Comparable) e1.getValue()).compareTo(e2.getValue());
			}
		});

		return entries;
	}

	/**
	 * Returns top <i>n</i> entries sorted by descending value. Ties broken by
	 * the key.
	 * 
	 * @param n
	 *            number of entries to return
	 * @return top <i>n</i> entries sorted by descending value
	 */
	public Entry<V>[] getEntriesSortedByValue(int n) {
		Entry<V>[] entries = getEntriesSortedByValue();

		if (entries == null)
			return null;

		if (entries.length < n)
			return entries;

		return Arrays.copyOfRange(entries, 0, n);
	}

}
