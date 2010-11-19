package edu.umd.cloud9.util;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.google.common.collect.Lists;

import edu.umd.cloud9.io.PairOfInts;

public class FrequencyDistributionOfInts extends Int2IntOpenHashMap {

	private static final long serialVersionUID = -8991144500446882265L;

	private long mSumOfFrequencies = 0;

	public void increment(int key) {
		if (containsKey(key)) {
			put(key, get(key) + 1);
		} else {
			put(key, 1);
		}
	}

	public void increment(int key, int cnt) {
		if (containsKey(key)) {
			put(key, get(key) + cnt);
		} else {
			put(key, cnt);
		}
	}

	public void decrement(int key) {
		if (containsKey(key)) {
			int v = get(key);
			if (v == 1) {
				remove(key);
			} else {
				put(key, this.get(key) - 1);
			}
		} else {
			throw new RuntimeException("Can't decrement non-existent event!");
		}
	}

	public void decrement(int key, int cnt) {
		if (containsKey(key)) {
			int v = get(key);
			if (v < cnt) {
				throw new RuntimeException("Can't decrement past zero!");
			} else if (v == cnt) {
				remove(key);
			} else {
				put(key, this.get(key) - cnt);
			}
		} else {
			throw new RuntimeException("Can't decrement non-existent event!");
		}
	}

	@Override
	public int put(int k, int v) {
		int rv = super.put(k, v);
		mSumOfFrequencies = mSumOfFrequencies - rv + v;

		return rv;
	}

	@Override
	public Integer put(Integer ok, Integer ov) {
		return put((int) ok, (int) ov);
	}

	@Override
	public int remove(int k) {
		int rv = super.remove(k);
		mSumOfFrequencies -= rv;

		return rv;
	}

	@Override
	public Integer remove(Object ok) {
		return this.remove((int) (Integer) ok);
	}

	public List<PairOfInts> getFrequencySortedEvents() {
		List<PairOfInts> list = Lists.newArrayList();

		for (Int2IntMap.Entry e : int2IntEntrySet()) {
			list.add(new PairOfInts(e.getIntKey(), e.getIntValue()));
		}

		// sort the entries
		Collections.sort(list, new Comparator<PairOfInts>() {
			public int compare(PairOfInts e1, PairOfInts e2) {
				if (e1.getRightElement() > e2.getRightElement()) {
					return -1;
				}

				if (e1.getRightElement() < e2.getRightElement()) {
					return 1;
				}

				if (e1.getLeftElement() == e2.getLeftElement()) {
					throw new RuntimeException("Event observed twice!");
				}

				return e1.getLeftElement() < e2.getLeftElement() ? -1 : 1;
			}
		});

		return list;
	}

	public List<PairOfInts> getFrequencySortedEvents(int n) {
		List<PairOfInts> list = getFrequencySortedEvents();
		return list.subList(0, n);
	}

	public List<PairOfInts> getSortedEvents() {
		List<PairOfInts> list = Lists.newArrayList();

		for (Int2IntMap.Entry e : int2IntEntrySet()) {
			list.add(new PairOfInts(e.getIntKey(), e.getIntValue()));
		}

		// sort the entries
		Collections.sort(list, new Comparator<PairOfInts>() {
			public int compare(PairOfInts e1, PairOfInts e2) {
				if (e1.getLeftElement() > e2.getLeftElement()) {
					return 1;
				}

				if (e1.getLeftElement() < e2.getLeftElement()) {
					return -1;
				}

				throw new RuntimeException("Event observed twice!");
			}
		});

		return list;
	}

	public List<PairOfInts> getSortedEvents(int n) {
		List<PairOfInts> list = getSortedEvents();
		return list.subList(0, n);
	}

	public int getNumberOfEvents() {
		return size();
	}

	public long getSumOfFrequencies() {
		return mSumOfFrequencies;
	}
}
