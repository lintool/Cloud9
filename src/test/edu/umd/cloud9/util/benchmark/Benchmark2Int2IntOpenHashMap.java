package edu.umd.cloud9.util.benchmark;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

import java.util.Random;

import edu.umd.cloud9.debug.MemoryUsageUtils;

public class Benchmark2Int2IntOpenHashMap {

	public static void main(String[] args) {
		int size = 5000000;
		long startTime;
		long duration;
		Random r = new Random();
		int[] ints = new int[size];

		long usedMemory1 = MemoryUsageUtils.getUsedMemory();

		System.out.println("Benchmarking Int2IntOpenHashMap...");
		Int2IntOpenHashMap map = new Int2IntOpenHashMap();

		startTime = System.currentTimeMillis();
		for (int i = 0; i < size; i++) {
			int k = r.nextInt(size);
			map.put(i, k);
			ints[i] = k;
		}
		duration = System.currentTimeMillis() - startTime;
		System.out.println(" Inserting " + size + " random entries: " + duration + " ms");

		startTime = System.currentTimeMillis();
		for (int i = 0; i < size; i++) {
			int v = map.get(i);

			if (v != ints[i])
				throw new RuntimeException("Values don't match!");
		}
		duration = System.currentTimeMillis() - startTime;
		System.out.println(" Accessing " + size + " random entries: " + duration + " ms");

		long usedMemory2 = MemoryUsageUtils.getUsedMemory();

		System.out.println("Used memory before: " + usedMemory1);
		System.out.println("Used memory after: " + usedMemory2);
		System.out.println("Total memory usage: " + (usedMemory2 - usedMemory1));
		System.out.println("Memory usage per map entry: "
				+ ((float) (usedMemory2 - usedMemory1) / size));
	}

}
