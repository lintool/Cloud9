package edu.umd.cloud9.util.benchmark;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

import java.util.Random;

public class BenchmarkRandomWalk1Int2IntOpenHashMap {

	private static int removals =0;

	public static void main(String[] args) {
		int size = 10000000;
		long startTime;
		long duration;
		Random r = new Random();

		System.out.println("Benchmarking Int2IntOpenHashMap...");
		Int2IntOpenHashMap map = new Int2IntOpenHashMap();
		startTime = System.currentTimeMillis();
		for (int i = 0; i < size; i++) {
			int k = r.nextInt(1000);
			boolean increment = r.nextBoolean();
			if ( increment ) {
				increment(map, k);
			} else {
				decrement(map, k);
				
			}
		}
		duration = System.currentTimeMillis() - startTime;

		System.out.println("removals: " + removals);
		System.out.println("Time taken: " + duration + " ms");
	}

	private static void increment(Int2IntMap map, int key) {
		if ( map.containsKey(key)) {
			map.put(key, map.get(key) + 1);
		} else {
			map.put(key, 1);
		}
	}

	private static void decrement(Int2IntMap map, int key) {
		if ( map.containsKey(key)) {
			int val = map.get(key);
			if ( val == 1 ) {
				removals++;
				map.remove(key);
			} else {
				map.put(key, val - 1);
			}
		}
	}
}
