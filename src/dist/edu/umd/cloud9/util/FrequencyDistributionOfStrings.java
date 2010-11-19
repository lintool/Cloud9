package edu.umd.cloud9.util;

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.google.common.collect.Lists;

import edu.umd.cloud9.io.PairOfStringInt;

public class FrequencyDistributionOfStrings extends Object2IntOpenHashMap<String> {

	private static final long serialVersionUID = -1167146372606430678L;

	private long mSumOfFrequencies = 0;

	public void increment(String key) {
		if (containsKey(key)) {
			put(key, get(key) + 1);
		} else {
			put(key, 1);
		}
	}

	public void increment(String key, int cnt) {
		if (containsKey(key)) {
			put(key, get(key) + cnt);
		} else {
			put(key, cnt);
		}
	}

	public void decrement(String key) {
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

	public void decrement(String key, int cnt) {
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

	public int get(String k) {
		return getInt(k);
	}

	@Override
	public int put(String k, int v) {
		int rv = super.put(k, v);
		mSumOfFrequencies = mSumOfFrequencies - rv + v;

		return rv;
	}

	@Override
	public Integer put(String ok, Integer ov) {
		return put(ok, (int) ov);
	}

	public int remove(String k) {
		int rv = super.remove(k);
		mSumOfFrequencies -= rv;

		return rv;
	}

	@Override
	public int removeInt(final Object k) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Integer remove(final Object ok) {
		throw new UnsupportedOperationException();
	}

	public List<PairOfStringInt> getFrequencySortedEvents() {
		List<PairOfStringInt> list = Lists.newArrayList();

		for (Object2IntMap.Entry<String> e : object2IntEntrySet()) {
			list.add(new PairOfStringInt((String) e.getKey(), e.getIntValue()));
		}

		// sort the entries
		Collections.sort(list, new Comparator<PairOfStringInt>() {
			public int compare(PairOfStringInt e1, PairOfStringInt e2) {
				if (e1.getRightElement() > e2.getRightElement()) {
					return -1;
				}

				if (e1.getRightElement() < e2.getRightElement()) {
					return 1;
				}

				return e1.getLeftElement().compareTo(e2.getLeftElement());
			}
		});

		return list;
	}

	public List<PairOfStringInt> getFrequencySortedEvents(int n) {
		List<PairOfStringInt> list = getFrequencySortedEvents();
		return list.subList(0, n);
	}

	public List<PairOfStringInt> getSortedEvents() {
		List<PairOfStringInt> list = Lists.newArrayList();

		for (Object2IntMap.Entry<String> e : object2IntEntrySet()) {
			list.add(new PairOfStringInt((String) e.getKey(), e.getIntValue()));
		}

		// sort the entries
		Collections.sort(list, new Comparator<PairOfStringInt>() {
			public int compare(PairOfStringInt e1, PairOfStringInt e2) {
				if (e1.getLeftElement().equals(e2.getLeftElement())) {
					throw new RuntimeException("Event observed twice!");
				}

				return e1.getLeftElement().compareTo(e1.getLeftElement());
			}
		});

		return list;
	}

	public List<PairOfStringInt> getSortedEvents(int n) {
		List<PairOfStringInt> list = getSortedEvents();
		return list.subList(0, n);
	}

	public int getNumberOfEvents() {
		return size();
	}

	public long getSumOfFrequencies() {
		return mSumOfFrequencies;
	}
}
