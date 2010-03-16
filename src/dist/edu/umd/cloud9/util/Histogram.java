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

/**
 * <p>
 * A class for keeping track of the number of times an event has been observed.
 * This is useful for counting things like distribution over POS tags, terms,
 * etc.
 * </p>
 * 
 * <p>
 * Example usage:
 * </p>
 * 
 * <pre>
 * Histogram&lt;String&gt; h = new Histogram&lt;String&gt;();
 * String[] terms = myString.split(&quot;\\s+&quot;);
 * 
 * for (String term : terms) {
 * 	h.count(term);
 * }
 * 
 * for (MapKI.Entry&lt;String&gt; e : h.entrySet()) {
 * 	System.out.println(e.getKey() + &quot;: &quot; + e.getValue());
 * 	// do something with e.getKey()
 * 	// do something with e.getValue()
 * }
 * </pre>
 * 
 * @param <T>
 *            type of object
 */
public class Histogram<T extends Comparable<T>> extends HMapKI<T> {

	private static final long serialVersionUID = 9190462865L;

	private int mTotalCount = 0;

	/**
	 * Constructs an <code>Histogram</code>.
	 */
	public Histogram() {
		super();
	}

	/**
	 * Resets this histogram, purging all observations and counts.
	 */
	public void clear() {
		super.clear();
		mTotalCount = 0;
	}

	/**
	 * Adds an instance to the set of observations.
	 * 
	 * @param instance
	 *            the instance observed
	 */
	public void count(T instance) {
		if (this.containsKey(instance)) {
			this.put(instance, this.get(instance) + 1);
		} else {
			this.put(instance, 1);
		}
		mTotalCount++;
	}

	/**
	 * Returns the number of times a particular instance has been observed.
	 * 
	 * @param inst
	 *            the instance
	 * @return the count of the instance
	 */
	public int getCount(T inst) {
		if (this.containsKey(inst)) {
			return this.get(inst);
		}

		return 0;
	}

	/**
	 * Returns the total number of observations.
	 * 
	 * @return the total number of observations
	 */
	public int getTotalCount() {
		return mTotalCount;
	}

	/**
	 * Prints each instance and how many times its been observed, sorted by the
	 * counts.
	 */
	public void printCounts() {
		for (MapKI.Entry<T> map : this.getEntriesSortedByValue()) {
			System.out.println(map.getValue() + "\t" + map.getKey());
		}
	}

}
