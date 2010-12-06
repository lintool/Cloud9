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

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import edu.umd.cloud9.io.PairOfInts;

/**
 * An implementation of a conditional frequency distribution for int
 * events, backed by a fastutil open hash map. This class keeps track of
 * frequencies using ints, so beware when dealing with a large number of
 * observations.
 *
 * @author Jimmy Lin
 *
 */
public class OpenConditionalFrequencyDistributionOfInts implements ConditionalFrequencyDistributionOfInts {

	private final Int2ObjectMap<OpenFrequencyDistributionOfInts> mDistributions = new Int2ObjectOpenHashMap<OpenFrequencyDistributionOfInts>();
	private final OpenFrequencyDistributionOfInts mMarginals = new OpenFrequencyDistributionOfInts();

	private long mSumOfAllFrequencies = 0;

	@Override
	public void set(int k, int cond, int v) {
		if (!mDistributions.containsKey(cond)) {
			OpenFrequencyDistributionOfInts fd = new OpenFrequencyDistributionOfInts();
			fd.set(k, v);
			mDistributions.put(cond, fd);
			mMarginals.increment(k, v);

			mSumOfAllFrequencies += v;
		} else {
			OpenFrequencyDistributionOfInts fd = mDistributions.get(cond);
			int rv = fd.get(k);

			fd.set(k, v);
			mDistributions.put(cond, fd);
			mMarginals.increment(k, -rv + v);

			mSumOfAllFrequencies = mSumOfAllFrequencies - rv + v;
		}
	}

	@Override
	public void increment(int k, int cond) {
		increment(k, cond, 1);
	}

	@Override
	public void increment(int k, int cond, int v) {
		int cur = get(k, cond);
		if (cur == 0) {
			set(k, cond, v);
		} else {
			set(k, cond, cur + v);
		}
	}

	@Override
	public int get(int k, int cond) {
		if ( !mDistributions.containsKey(cond)) {
			return 0;
		}

		return mDistributions.get(cond).get(k);
	}

	@Override
	public int getMarginalCount(int k) {
		return mMarginals.get(k);
	}

	@Override
	public OpenFrequencyDistributionOfInts getConditionalDistribution(int cond) {
		if ( mDistributions.containsKey(cond) ) {
			return mDistributions.get(cond);
		}

		return new OpenFrequencyDistributionOfInts();
	}

	@Override
	public long getSumOfAllFrequencies() {
		return mSumOfAllFrequencies;
	}

	@Override
	public void check() {
		OpenFrequencyDistributionOfInts m = new OpenFrequencyDistributionOfInts();

		long totalSum = 0;
		for (OpenFrequencyDistributionOfInts fd : mDistributions.values()) {
			long conditionalSum = 0;

			for (PairOfInts pair : fd.getSortedEvents()) {
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

		for (PairOfInts e : m.getSortedEvents()) {
			if ( e.getRightElement() != mMarginals.get(e.getLeftElement()) ) {
				throw new RuntimeException("Internal Error!");
			}
		}

		for (PairOfInts e : m.getSortedEvents()) {
			if ( e.getRightElement() != m.get(e.getLeftElement()) ) {
				throw new RuntimeException("Internal Error!");
			}
		}
	}
}
