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
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * A class for keeping track of the number of times an object has been
 * encountered. This is useful for counting things in a stream, e.g., POS tags,
 * terms, etc.
 */
public class InstanceCounter<T extends Comparable<T>> {

	// internal representation---although the scores are doubles, counts are
	// obviously integers
	private ScoreSortedMap<T> mMap;

	private int mTotalCount = 0;

	/**
	 * Constructs an <code>InstanceCounter</code>.
	 */
	public InstanceCounter() {
		mMap = new ScoreSortedMap<T>();
	}

	/**
	 * Adds an instance to the set of observations.
	 * 
	 * @param instance
	 *            the instance observed
	 */
	public void count(T instance) {
		if (mMap.containsKey(instance)) {
			mMap.put(instance, mMap.get(instance) + 1);
		} else {
			mMap.put(instance, 1.0);
		}
		mTotalCount++;
	}

	/**
	 * Prints each instance and how many times its been observed, sorted by the
	 * counts.
	 */
	public void printCounts() {
		for (Map.Entry<T, Double> map : mMap.getSortedEntries()) {
			System.out.println(map.getValue().intValue() + "\t" + map.getKey());
		}
	}

	/**
	 * Returns a list of <code>InstanceCount</code> objects, sorted by count.
	 */
	public List<InstanceCount> getCounts() {
		List<InstanceCount> l = new ArrayList<InstanceCount>();

		for (Map.Entry<T, Double> map : mMap.getSortedEntries()) {
			l.add(new InstanceCount(map.getKey(), map.getValue().intValue(),
					map.getValue() / (double) mTotalCount));
		}

		return Collections.unmodifiableList(l);
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
	 * Returns the number of times a particular instance has been observed.
	 * 
	 * @param inst
	 *            the instance
	 * @return the count of the instance
	 */
	public int getCount(T inst) {
		if (mMap.containsKey(inst)) {
			return mMap.get(inst).intValue();
		}

		return 0;
	}

	/**
	 * Returns a collection of all objects observed, sorted by their natural
	 * order.
	 * 
	 * @return a collection of all objects observed, sorted by their natural
	 *         order.
	 */
	public SortedSet<T> getObservedObjects() {
		SortedSet<T> t = new TreeSet<T>();

		for (T obj : mMap.keySet()) {
			t.add(obj);
		}

		return t;
	}

	/**
	 * A class that holds an instance, its count, and its frequency.
	 */
	public class InstanceCount {
		private T mInstance;

		private int mCount;

		private double mFreq;

		private InstanceCount(T instance, int cnt, double freq) {
			mInstance = instance;
			mCount = cnt;
			mFreq = freq;
		}

		/**
		 * Returns the instance.
		 */
		public T getInstance() {
			return mInstance;
		}

		/**
		 * Returns the number of times the instance has been observed.
		 */
		public int getCount() {
			return mCount;
		}

		/**
		 * Returns the frequency that this instance has been observed. Frequency
		 * is the count divided by the total number of observed instances.
		 */
		public double getFrequency() {
			return mFreq;
		}
	}

	public void clear() {
		mMap.clear();
	}

}
