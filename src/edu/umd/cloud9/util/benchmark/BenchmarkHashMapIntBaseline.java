package edu.umd.cloud9.util.benchmark;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class BenchmarkHashMapIntBaseline {

	public static void main(String[] args) {
		int size = 10000000;
		long startTime;
		double duration;
		Random r = new Random();
		int[] ints = new int[size];

		System.out.println("Benchmarking HashMap<Integer, Integer>...");
		Map<Integer, Integer> map = new HashMap<Integer, Integer>();

		startTime = System.currentTimeMillis();
		for (int i = 0; i < size; i++) {
			int k = r.nextInt(size);
			map.put(i, k);
			ints[i] = k;
		}
		duration = (System.currentTimeMillis() - startTime) / 1000.0;
		System.out.println(" Inserting " + size + " random entries: " + duration + " seconds");

		startTime = System.currentTimeMillis();
		for (int i = 0; i < size; i++) {
			int v = map.get(i);

			if (v != ints[i])
				throw new RuntimeException("Values don't match!");
		}
		duration = (System.currentTimeMillis() - startTime) / 1000.0;
		System.out.println(" Accessing " + size + " random entries: " + duration + " seconds");

	}
}
