package edu.umd.cloud9.io;

import java.util.HashMap;
import java.util.Map;

public class BenchmarkHashMap {

	@SuppressWarnings("unused")
	public static void main(String[] args) {
		long startTime;
		double duration;

		int trials = 100000000;

		System.out.println(trials + " accesses to a 3-element collection:");
		Map<String, Integer> map = new HashMap<String, Integer>();
		map.put("field1", 1);
		map.put("field2", 2);
		map.put("field3", 2);

		startTime = System.currentTimeMillis();
		for (int i = 0; i < trials; i++) {
			int tmp = map.get("field1");
		}
		duration = (System.currentTimeMillis() - startTime) / 1000.0;
		System.out.println(" for HashMap: " + duration + " seconds");

		int[] array = { 1, 2, 3 };

		startTime = System.currentTimeMillis();
		for (int i = 0; i < trials; i++) {
			int tmp = array[1];
		}
		duration = (System.currentTimeMillis() - startTime) / 1000.0;
		System.out.println(" for array: " + duration + " seconds");

	}
}
