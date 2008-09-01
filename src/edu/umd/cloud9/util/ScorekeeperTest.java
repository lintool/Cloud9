/*
 * Cloud9: A MapReduce Library for Hadoop
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
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
		Iterator<Map.Entry<String, Double>> iter = map.getSortedEntries().iterator();

		e = iter.next();
		assertEquals(e.getKey(), "d");
		assertTrue(e.getValue() == 5.0d);

		e = iter.next();
		assertEquals(e.getKey(), "b");
		assertTrue(e.getValue() == 3.0d);

		e = iter.next();
		assertEquals(e.getKey(), "c");
		assertTrue(e.getValue() == 2.5d);

		e = iter.next();
		assertEquals(e.getKey(), "a");
		assertTrue(e.getValue() == 1.0d);
	}

	public void test2() {
		Scorekeeper<String, Double> map = new Scorekeeper<String, Double>();

		map.put("a", -1.0d);
		map.put("b", -3.0d);
		map.put("c", -2.5d);
		map.put("d", -5.0d);

		Map.Entry<String, Double> e;
		Iterator<Map.Entry<String, Double>> iter = map.getSortedEntries().iterator();

		e = iter.next();
		assertEquals(e.getKey(), "a");
		assertTrue(e.getValue() == -1.0d);

		e = iter.next();
		assertEquals(e.getKey(), "c");
		assertTrue(e.getValue() == -2.5d);

		e = iter.next();
		assertEquals(e.getKey(), "b");
		assertTrue(e.getValue() == -3.0d);

		e = iter.next();
		assertEquals(e.getKey(), "d");
		assertTrue(e.getValue() == -5.0d);
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
