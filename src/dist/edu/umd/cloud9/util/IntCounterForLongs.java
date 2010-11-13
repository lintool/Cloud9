package edu.umd.cloud9.util;

import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;

public class IntCounterForLongs extends Long2IntOpenHashMap {

	private static final long serialVersionUID = -6710305220596539780L;

	public void increment(long key) {
		if (containsKey(key)) {
			put(key, get(key) + 1);
		} else {
			put(key, 1);
		}
	}

	public void decrement(long key) {
		if (containsKey(key)) {
			int v = get(key);
			if ( v == 1 ) {
				remove(key);
			} else {
				put(key, this.get(key) - 1);
			}
		}
	}
}
