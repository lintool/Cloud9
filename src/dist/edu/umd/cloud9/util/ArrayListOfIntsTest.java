package edu.umd.cloud9.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Random;

import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

public class ArrayListOfIntsTest {

	@Test
	public void testBasic1() {
		int size = 100000;
		Random r = new Random();
		int[] ints = new int[size];

		ArrayListOfInts list = new ArrayListOfInts();
		for (int i = 0; i < size; i++) {
			int k = r.nextInt(size);
			list.add(k);
			ints[i] = k;
		}

		for (int i = 0; i < size; i++) {
			int v = list.get(i);

			assertEquals(ints[i], v);
		}

	}

	@Test
	public void testUpdate() {
		int size = 100000;
		Random r = new Random();
		int[] ints = new int[size];

		ArrayListOfInts list = new ArrayListOfInts();
		for (int i = 0; i < size; i++) {
			int k = r.nextInt(size);
			list.add(k);
			ints[i] = k;
		}

		assertEquals(size, list.size());

		for (int i = 0; i < size; i++) {
			list.set(i, ints[i] + 1);
		}

		assertEquals(size, list.size());

		for (int i = 0; i < size; i++) {
			int v = list.get(i);

			assertEquals(ints[i] + 1, v);
		}

	}

	@Test
	public void testTrim1() {
		int size = 89;
		Random r = new Random();
		int[] ints = new int[size];

		ArrayListOfInts list = new ArrayListOfInts();
		for (int i = 0; i < size; i++) {
			int k = r.nextInt(size);
			list.add(k);
			ints[i] = k;
		}

		for (int i = 0; i < size; i++) {
			int v = list.get(i);

			assertEquals(ints[i], v);
		}

		int[] rawArray = list.getArray();
		int lenBefore = rawArray.length;
		
		list.trimToSize();
		int[] rawArrayAfter = list.getArray();
		int lenAfter = rawArrayAfter.length;

		assertEquals(89, lenAfter);
		assertTrue(lenBefore > lenAfter);
	}
	
	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(ArrayListOfIntsTest.class);
	}

}