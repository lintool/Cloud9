package edu.umd.cloud9.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

public class HMap2IITest {

	@Test
	public void testBasic1() {
		MapII map = new HMap2II();
		
		map.put(10, 1);
		assertEquals(1, map.get(10));
		
		map.put(10, 2);
		assertEquals(2, map.get(10));
		
	}
	
	@Test
	public void testBasic2() {

		int size = 1000;
		int range = 100000;
		Random r = new Random();
		
		Map<Integer, Integer> ref = new HashMap<Integer, Integer>(); 

		MapII map = new HMap2II();
		for (int i = 0; i < size; i++) {
			int k = r.nextInt(range);
			int v = r.nextInt(range);

			map.put(k, v);
			ref.put(k, v);
		}

		for ( int rk : ref.keySet()) {
			int rv = ref.get(rk);
			int v = map.get(rk);
			
			assertEquals(rv, v);
		}

	}

	/*
	@Test
	public void testBasic3() {

		int size = 100000;
		Random r = new Random();
		int[] ints = new int[size];

		MapII map = new HMap2II();
		for (int i = 0; i < size; i++) {
			int k = r.nextInt(size);
			map.put(i, k);
			ints[i] = k;
		}

		for (int i = 0; i < size; i++) {
			int v = map.get(i);

			assertEquals(ints[i], v);
			assertTrue(map.containsKey(i));
		}

	}*/

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(HMap2IITest.class);
	}

}