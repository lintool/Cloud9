package edu.umd.cloud9.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * A Map that holds scores (doubles) associated with each object (key) and
 * supports iteration by score. Many applications call for this type of
 * functionality: the ability to associate scores with objects coupled with the
 * ability to sort entries by their scores.
 * 
 * @param <K>
 *            type of key
 */
public class ScoreSortedMap<K extends Comparable<K>> extends HashMap<K, Double> {

	private static final long serialVersionUID = 2983410765L;

	/**
	 * Constructs a <code>ScoreSortedMap</code>.
	 */
	public ScoreSortedMap() {
		super();
	}

	/**
	 * Returns the all entries sorted by scores.
	 * 
	 * @return a sorted set view of the entries sorted by scores
	 */
	public SortedSet<Map.Entry<K, Double>> getSortedEntries() {
		SortedSet<Map.Entry<K, Double>> entries = new TreeSet<Map.Entry<K, Double>>(
				new Comparator<Map.Entry<K, Double>>() {
					public int compare(Map.Entry<K, Double> e1,
							Map.Entry<K, Double> e2) {
						if (e1.getValue() > e2.getValue()) {
							return -1;
						} else if (e1.getValue() < e2.getValue()) {
							return 1;
						}
						return e1.getKey().compareTo(e2.getKey());
					}
				});

		for (Map.Entry<K, Double> entry : this.entrySet()) {
			entries.add(entry);
		}

		return Collections.unmodifiableSortedSet(entries);
	}

	/**
	 * Returns the <i>n</i> top entries sorted by scores.
	 * 
	 * @param n
	 *            number of entries to retrieve
	 * @return a Set view of the entries sorted by scores
	 */
	public SortedSet<Map.Entry<K, Double>> getSortedEntries(int n) {

		SortedSet<Map.Entry<K, Double>> entries = new TreeSet<Map.Entry<K, Double>>(
				new Comparator<Map.Entry<K, Double>>() {
					public int compare(Map.Entry<K, Double> e1,
							Map.Entry<K, Double> e2) {
						if (e1.getValue() > e2.getValue()) {
							return -1;
						} else if (e1.getValue() < e2.getValue()) {
							return 1;
						}
						return e1.getKey().compareTo(e2.getKey());
					}
				});

		int cnt = 0;
		for (Map.Entry<K, Double> entry : getSortedEntries()) {
			entries.add(entry);
			cnt++;
			if (cnt >= n)
				break;
		}

		return Collections.unmodifiableSortedSet(entries);
	}

	/**
	 * Returns the top-scoring entry.
	 * 
	 * @return the top-scoring entry
	 */
	public Map.Entry<K, Double> getTopEntry() {
		return getSortedEntries().first();
	}

	/**
	 * Returns the <i>i</i>th scoring entry.
	 * 
	 * @param i
	 *            the rank
	 * @return the <i>i</i>th scoring entry
	 */
	public Map.Entry<K, Double> getEntryByRank(int i) {
		if (i > this.size())
			throw new NoSuchElementException("Error: index out of bounds");

		Iterator<Map.Entry<K, Double>> iter = getSortedEntries().iterator();

		int n = 0;
		while (n++ < i - 1)
			iter.next();

		return iter.next();
	}

	/**
	 * Returns a list of the keys, sorted by score.
	 * 
	 * @return a list of the keys, sorted by score
	 */
	public List<K> getSortedKeys() {
		List<K> list = new ArrayList<K>();

		for (Map.Entry<K, Double> entry : getSortedEntries()) {
			list.add(entry.getKey());
		}

		return list;
	}

	/**
	 * Normalizes all scores to a value between zero and one. Note that if all
	 * keys have a single score, no action is performed.
	 */
	public void normalizeScores() {
		double max = Double.NEGATIVE_INFINITY;
		double min = Double.POSITIVE_INFINITY;

		for (Map.Entry<K, Double> entry : this.entrySet()) {
			double score = entry.getValue();

			if (score > max)
				max = score;

			if (score < min)
				min = score;

		}

		// if there's only one value, then meaningless to normalize
		if (max == min)
			return;

		for (Map.Entry<K, Double> entry : this.entrySet()) {
			K cur = entry.getKey();
			double score = entry.getValue();

			this.put(cur, (score - min) / (max - min));
		}

	}

	/**
	 * Returns a new <code>ScoreSortedMap</code> where the score of each key
	 * in this object has been linearly interpolated with scores drawn from
	 * another <code>ScoreSortedMap</code>. A weight of <code>lambda</code>
	 * is given to the score from this object, and a weight of (1-<code>lambda</code>)
	 * is given to the score from the other <code>ScoreSortedMap</code>. Both
	 * <code>ScoreSortedMap</code>s are first normalized. Note that if a key
	 * is not contained in this object, but present in the other
	 * <code>ScoreSortedMap</code>, it will <b>not</b> be present in the new
	 * <code>ScoreSortedMap</code>.
	 * 
	 * @param s
	 *            the other <code>ScoreSortedMap</code>
	 * @param lambda
	 *            weight assigned to scores from this object
	 * @return a new <code>ScoreSortedMap</code> with linearly-interpolated
	 *         scores
	 */
	public ScoreSortedMap<K> linearInterpolationWith(ScoreSortedMap<K> s,
			double lambda) {
		this.normalizeScores();
		s.normalizeScores();

		ScoreSortedMap<K> entries = new ScoreSortedMap<K>();

		for (Map.Entry<K, Double> entry : getSortedEntries()) {
			double score1 = entry.getValue();
			double score2 = 0.0d;

			if (s.containsKey(entry.getKey())) {
				score2 = s.get(entry.getKey());
			}

			double newscore = lambda * score1 + (1 - lambda) * score2;
			// System.out.println(lambda + " * " + score1 + " + (1-" + lambda +
			// ") * " + score2 + " = " + newscore);
			entries.put(entry.getKey(), newscore);
		}

		return entries;
	}

}
