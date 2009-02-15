package edu.umd.cloud9.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Random;

import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

public class HMapKITest {

	@Test
	public void testBasic1() {

		int size = 100000;
		Random r = new Random();
		int[] ints = new int[size];

		MapKI<Integer> map = new HMapKI<Integer>();
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

	}

	@Test
	public void testBasic2() {

		int size = 100000;
		Random r = new Random();
		int[] ints = new int[size];
		String[] strings = new String[size];

		MapKI<String> map = new HMapKI<String>();
		for (int i = 0; i < size; i++) {
			int k = r.nextInt(size);
			String s = new Integer(k).toString();
			map.put(s, k);
			ints[i] = k;
			strings[i] = s;
		}

		for (int i = 0; i < size; i++) {
			int v = map.get(strings[i]);

			assertEquals(ints[i], v);
			assertTrue(map.containsKey(strings[i]));
		}

	}
	
	@Test
	public void testUpdate() {

		int size = 100000;
		Random r = new Random();
		int[] ints = new int[size];

		MapKI<Integer> map = new HMapKI<Integer>();
		for (int i = 0; i < size; i++) {
			int k = r.nextInt(size);
			map.put(i, k);
			ints[i] = k;
		}

		assertEquals(size, map.size());

		for (int i = 0; i < size; i++) {
			map.put(i, ints[i]+1);
		}

		assertEquals(size, map.size());

		for (int i = 0; i < size; i++) {
			int v = map.get(i);

			assertEquals(ints[i] + 1, v);
			assertTrue(map.containsKey(i));
		}

	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(HMapKITest.class);
	}

}