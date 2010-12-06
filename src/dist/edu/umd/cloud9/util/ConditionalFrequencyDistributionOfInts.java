package edu.umd.cloud9.util;

/**
 * A conditional frequency distribution for int events. This interface
 * specifies keeping track of frequencies using ints, so beware when dealing
 * with a large number of observations.
 *
 * @author Jimmy Lin
 *
 */
public interface ConditionalFrequencyDistributionOfInts {

	/**
	 * Sets the observed frequency of <code>k</code> conditioned on <code>cond</code> to <code>v</code>.
	 */
	public void set(int k, int cond, int v);

	/**
	 * Increments the observed frequency of <code>k</code> conditioned on <code>cond</code>.
	 */
	public void increment(int k, int cond);

	/**
	 * Increments the observed frequency of <code>k</code> conditioned on <code>cond</code> by <code>v</code>.
	 */
	public void increment(int k, int cond, int v);

	/**
	 * Returns the observed frequency of <code>k</code> conditioned on <code>cond</code>.
	 */
	public int get(int k, int cond);

	/**
	 * Returns the marginal count of <code>k</code>. That is, sum of counts of
	 * <code>k</code> conditioned on all <code>cond</code>.
	 */
	public int getMarginalCount(int k);

	/**
	 * Returns the frequency distribution conditioned on <code>cond</code>.
	 */
	public FrequencyDistributionOfInts getConditionalDistribution(int cond);

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
