/*
 * URA (Uniform Retrieval Architecture)
 * Copyright (C) 2004-2007, Jimmy Lin
 * 
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, please visit
 * http://www.gnu.org/licenses/gpl.htm
 * 
 */

package edu.umd.cloud9.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import junit.framework.JUnit4TestAdapter;

import org.junit.Test;


public class ScorekeeperTest {

	@Test
	public void test1() {
		Scorekeeper<String, Double> map = new Scorekeeper<String, Double>();

		map.put("a", 1.0d);
		map.put("b", 3.0d);
		map.put("c", 2.5d);
		map.put("d", 5.0d);

		Map.Entry<String, Double> e;
		Iterator<Map.Entry<String, Double>> iter = map.getSortedEntries()
				.iterator();

		e = iter.next();
		assertEquals(e.getKey(), "d");
		assertTrue(e.getValue()==5.0d);

		e = iter.next();
		assertEquals(e.getKey(), "b");
		assertTrue(e.getValue()== 3.0d);

		e = iter.next();
		assertEquals(e.getKey(), "c");
		assertTrue(e.getValue()==2.5d);

		e = iter.next();
		assertEquals(e.getKey(), "a");
		assertTrue(e.getValue()==1.0d);
	}

	public void test2() {
		Scorekeeper<String, Double> map = new Scorekeeper<String, Double>();

		map.put("a", -1.0d);
		map.put("b", -3.0d);
		map.put("c", -2.5d);
		map.put("d", -5.0d);

		Map.Entry<String, Double> e;
		Iterator<Map.Entry<String, Double>> iter = map.getSortedEntries()
				.iterator();

		e = iter.next();
		assertEquals(e.getKey(), "a");
		assertTrue(e.getValue()==-1.0d);

		e = iter.next();
		assertEquals(e.getKey(), "c");
		assertTrue(e.getValue()== -2.5d);

		e = iter.next();
		assertEquals(e.getKey(), "b");
		assertTrue(e.getValue()== -3.0d);

		e = iter.next();
		assertEquals(e.getKey(), "d");
		assertTrue(e.getValue() == -5.0d);
	}

	@Test
	public void testNormalize1() {
		Scorekeeper<String, Double> map = new Scorekeeper<String, Double>();

		map.put("a", 1.0d);
		map.put("b", 3.0d);
		map.put("c", 2.5d);
		map.put("d", 5.0d);

		map.normalizeScores();
		
		Map.Entry<String, Double> e;
		Iterator<Map.Entry<String, Double>> iter = map.getSortedEntries()
				.iterator();

		e = iter.next();
		assertEquals(e.getKey(), "d");
		assertTrue(e.getValue()== 1.0d);

		e = iter.next();
		assertEquals(e.getKey(), "b");
		assertTrue(e.getValue()==0.5d);

		e = iter.next();
		assertEquals(e.getKey(), "c");
		assertTrue(e.getValue()==0.375d);

		e = iter.next();
		assertEquals(e.getKey(), "a");
		assertTrue(e.getValue()==0.0d);
	}

	@Test
	public void testNormalize2() {
		Scorekeeper<String, Double> map = new Scorekeeper<String, Double>();

		map.put("a", -1.0d);
		map.put("b", -3.0d);
		map.put("c", -2.5d);
		map.put("d", -5.0d);

		map.normalizeScores();

		Map.Entry<String, Double> e;
		Iterator<Map.Entry<String, Double>> iter = map.getSortedEntries()
				.iterator();

		e = iter.next();
		assertEquals(e.getKey(), "a");
		assertTrue(e.getValue()==1.0d);

		e = iter.next();
		assertEquals(e.getKey(), "c");
		assertTrue(e.getValue()== 0.625d);

		e = iter.next();
		assertEquals(e.getKey(), "b");
		assertTrue(e.getValue()==0.5d);

		e = iter.next();
		assertEquals(e.getKey(), "d");
		assertTrue(e.getValue()==0.0d);
	}
	

	@Test(expected = NoSuchElementException.class)
	public void testNthEntry() {
		Scorekeeper<String, Double> map = new Scorekeeper<String, Double>();

		map.put("a", 1.0d);
		map.put("b", 3.0d);
		map.put("c", 2.5d);
		map.put("d", 5.0d);

		assertEquals(map.getTopEntry().getKey(), "d");
		assertEquals(map.getEntryByRank(1).getKey(), "d");
		assertEquals(map.getEntryByRank(2).getKey(), "b");
		assertEquals(map.getEntryByRank(3).getKey(), "c");
		assertEquals(map.getEntryByRank(4).getKey(), "a");
		
		System.out.println(map.getEntryByRank(5).getKey());
	}
	
	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(ScorekeeperTest.class);
	}

}
