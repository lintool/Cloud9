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

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;

/**
 * An implementation of a conditional frequency distribution for arbitrary
 * events, backed by a fastutil map. One possible use is a bigram language
 * model. This class keeps track of frequencies using ints, so beware when
 * dealing with a large number of observations.
 *
 * @author Jimmy Lin
 *
 */
public class ConditionalFrequencyDistribution<K extends Comparable<K>> {

	private final Object2ObjectMap<K, FrequencyDistribution<K>> mDistributions = new Object2ObjectOpenHashMap<K, FrequencyDistribution<K>>();
	private final FrequencyDistribution<K> mMarginals = new FrequencyDistribution<K>();

	private long mSumOfAllFrequencies = 0;

	/**
	 * Sets the observed frequency of <code>k</code> conditioned on <code>cond</code> to <code>v</code>.
	 */
	public void put(K k, K cond, int v) {
		if (!mDistributions.containsKey(cond)) {
			FrequencyDistribution<K> fd = new FrequencyDistribution<K>();
			fd.put(k, v);
			mDistributions.put(cond, fd);
			mMarginals.increment(k, v);

			mSumOfAllFrequencies += v;
		} else {
			FrequencyDistribution<K> fd = mDistributions.get(cond);
			int rv = fd.get(k);

			fd.put(k, v);
			mDistributions.put(cond, fd);
			mMarginals.increment(k, -rv + v);

			mSumOfAllFrequencies = mSumOfAllFrequencies - rv + v;
		}
	}

	/**
	 * Increments the observed frequency of <code>k</code> conditioned on <code>cond</code>.
	 */
	public void increment(K k, K cond) {
		increment(k, cond, 1);
	}

	/**
	 * Increments the observed frequency of <code>k</code> conditioned on <code>cond</code> by <code>v</code>.
	 */
	public void increment(K k, K cond, int v) {
		int cur = get(k, cond);
		if (cur == 0) {
			put(k, cond, v);
		} else {
			put(k, cond, cur + v);
		}
	}

	/**
	 * Returns the observed frequency of <code>k</code> conditioned on <code>cond</code>.
	 */
	public int get(K k, K cond) {
		if ( !mDistributions.containsKey(cond)) {
			return 0;
		}

		return mDistributions.get(cond).get(k);
	}

	/**
	 * Returns the marginal count of <code>k</code>. That is, sum of counts of
	 * <code>k</code> conditioned on all <code>cond</code>.
	 */
	public int getMarginalCount(K k) {
		return mMarginals.get(k);
	}

	/**
	 * Returns the frequency distribution conditioned on <code>cond</code>.
	 */
	public FrequencyDistribution<K> getConditionalDistribution(K cond) {
		if ( mDistributions.containsKey(cond) ) {
			return mDistributions.get(cond);
		}

		return new FrequencyDistribution<K>();
	}

	/**
	 * Returns the sum of all frequencies.
	 */
	public long getSumOfAllFrequencies() {
		return mSumOfAllFrequencies;
	}

	/**
	 * Performs an internal consistency check of this data structure. An
	 * exception will be thrown if an error is found.
	 */
	public void check() {
		FrequencyDistribution<K> m = new FrequencyDistribution<K>();

		long totalSum = 0;
		for (FrequencyDistribution<K> fd : mDistributions.values()) {
			long conditionalSum = 0;

			for (PairOfObjectInt<K> pair : fd.getSortedEvents()) {
				conditionalSum += pair.getRightElement();
				m.increment(pair.getLeftElement(), pair.getRightElement());
			}

			if (conditionalSum != fd.getSumOfFrequencies()) {
				throw new RuntimeException("Internal Error!");
			}
			totalSum += fd.getSumOfFrequencies();
		}

		if (totalSum != getSumOfAllFrequencies()) {
			throw new RuntimeException("Internal Error! Got " + totalSum + ", Expected "	+ getSumOfAllFrequencies());
		}

		for (Object2IntMap.Entry<K> e : m.object2IntEntrySet()) {
			if ( e.getIntValue() != mMarginals.get(e.getKey()) ) {
				throw new RuntimeException("Internal Error!");
			}
		}

		for (Object2IntMap.Entry<K> e : mMarginals.object2IntEntrySet()) {
			if ( e.getIntValue() != m.get(e.getKey()) ) {
				throw new RuntimeException("Internal Error!");
			}
		}
	}
}
