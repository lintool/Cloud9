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
 * A conditional frequency distribution for arbitrary events. This interface
 * specifies keeping track of frequencies using ints, so beware when dealing
 * with a large number of observations.
 *
 * @author Jimmy Lin
 *
 */
public interface ConditionalFrequencyDistribution<K extends Comparable<K>> {

	/**
	 * Sets the observed frequency of <code>k</code> conditioned on <code>cond</code> to <code>v</code>.
	 */
	public void set(K k, K cond, int v);

	/**
	 * Increments the observed frequency of <code>k</code> conditioned on <code>cond</code>.
	 */
	public void increment(K k, K cond);

	/**
	 * Increments the observed frequency of <code>k</code> conditioned on <code>cond</code> by <code>v</code>.
	 */
	public void increment(K k, K cond, int v);

	/**
	 * Returns the observed frequency of <code>k</code> conditioned on <code>cond</code>.
	 */
	public int get(K k, K cond);

	/**
	 * Returns the marginal count of <code>k</code>. That is, sum of counts of
	 * <code>k</code> conditioned on all <code>cond</code>.
	 */
	public int getMarginalCount(K k);

	/**
	 * Returns the frequency distribution conditioned on <code>cond</code>.
	 */
	public FrequencyDistribution<K> getConditionalDistribution(K cond);

	/**
	 * Returns the sum of all frequencies.
	 */
	public long getSumOfAllFrequencies();

	/**
	 * Performs an internal consistency check of this data structure. An
	 * exception will be thrown if an error is found.
	 */
	public void check();
}
