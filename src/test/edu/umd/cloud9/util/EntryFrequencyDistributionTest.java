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

import java.util.List;

import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

public class EntryFrequencyDistributionTest {

	@Test
	public void test1() {
		FrequencyDistribution<String> fd = new EntryFrequencyDistribution<String>();

		assertEquals(0, fd.get("a"));

		fd.increment("a");
		fd.increment("b");
		fd.increment("c");
		fd.increment("b");
		fd.increment("c");
		fd.increment("c");

		assertEquals(3, fd.getNumberOfEvents());
		assertEquals(6, fd.getSumOfFrequencies());

		assertEquals(1, fd.get("a"));
		assertEquals(2, fd.get("b"));
		assertEquals(3, fd.get("c"));

		fd.decrement("c");

		assertEquals(3, fd.getNumberOfEvents());
		assertEquals(5, fd.getSumOfFrequencies());

		assertEquals(1, fd.get("a"));
		assertEquals(2, fd.get("b"));
		assertEquals(2, fd.get("c"));

		fd.decrement("a");

		assertEquals(2, fd.getNumberOfEvents());
		assertEquals(4, fd.getSumOfFrequencies());

		assertEquals(0, fd.get("a"));
		assertEquals(2, fd.get("b"));
		assertEquals(2, fd.get("c"));
	}

	@Test
	public void test2() {
		FrequencyDistribution<String> fd = new EntryFrequencyDistribution<String>();

		fd.increment("a");
		fd.increment("a");
		fd.increment("b");
		fd.increment("c");

		assertEquals(3, fd.getNumberOfEvents());
		assertEquals(4, fd.getSumOfFrequencies());

		assertEquals(2, fd.get("a"));
		assertEquals(1, fd.get("b"));
		assertEquals(1, fd.get("c"));

		fd.set("d", 5);

		assertEquals(4, fd.getNumberOfEvents());
		assertEquals(9, fd.getSumOfFrequencies());

		assertEquals(2, fd.get("a"));
		assertEquals(1, fd.get("b"));
		assertEquals(1, fd.get("c"));
		assertEquals(5, fd.get("d"));

		fd.set("a", 5);

		assertEquals(4, fd.getNumberOfEvents());
		assertEquals(12, fd.getSumOfFrequencies());

		assertEquals(5, fd.get("a"));
		assertEquals(1, fd.get("b"));
		assertEquals(1, fd.get("c"));
		assertEquals(5, fd.get("d"));

		fd.increment("c");
		fd.increment("c");
		fd.increment("c");

		assertEquals(4, fd.getNumberOfEvents());
		assertEquals(15, fd.getSumOfFrequencies());

		assertEquals(5, fd.get("a"));
		assertEquals(1, fd.get("b"));
		assertEquals(4, fd.get("c"));
		assertEquals(5, fd.get("d"));

		fd.set("c", 1);

		assertEquals(4, fd.getNumberOfEvents());
		assertEquals(12, fd.getSumOfFrequencies());

		assertEquals(5, fd.get("a"));
		assertEquals(1, fd.get("b"));
		assertEquals(1, fd.get("c"));
		assertEquals(5, fd.get("d"));
	}


	@Test(expected = RuntimeException.class)
	public void testFailedDecrement1() {
		FrequencyDistribution<String> fd = new EntryFrequencyDistribution<String>();

		fd.increment("a");

		assertEquals(1, fd.getNumberOfEvents());
		assertEquals(1, fd.getSumOfFrequencies());
		assertEquals(1, fd.get("a"));

		fd.decrement("a");

		assertEquals(0, fd.getNumberOfEvents());
		assertEquals(0, fd.getSumOfFrequencies());
		assertEquals(0, fd.get("a"));

		fd.decrement("a");
	}

	@Test(expected = RuntimeException.class)
	public void testFailedDecrement2() {
		FrequencyDistribution<String> fd = new EntryFrequencyDistribution<String>();

		fd.increment("a", 1000);

		assertEquals(1, fd.getNumberOfEvents());
		assertEquals(1000, fd.getSumOfFrequencies());
		assertEquals(1000, fd.get("a"));

		fd.decrement("a", 997);

		assertEquals(1, fd.getNumberOfEvents());
		assertEquals(3, fd.getSumOfFrequencies());
		assertEquals(3, fd.get("a"));

		fd.decrement("a", 3);

		assertEquals(0, fd.getNumberOfEvents());
		assertEquals(0, fd.getSumOfFrequencies());
		assertEquals(0, fd.get("a"));

		fd.increment("a", 3);
		fd.decrement("a", 4);
	}

	@Test
	public void testMultiIncrementDecrement() {
		FrequencyDistribution<String> fd = new EntryFrequencyDistribution<String>();

		fd.increment("a", 2);
		fd.increment("b", 3);
		fd.increment("c", 4);

		assertEquals(3, fd.getNumberOfEvents());
		assertEquals(9, fd.getSumOfFrequencies());

		assertEquals(2, fd.get("a"));
		assertEquals(3, fd.get("b"));
		assertEquals(4, fd.get("c"));

		fd.decrement("b", 2);

		assertEquals(3, fd.getNumberOfEvents());
		assertEquals(7, fd.getSumOfFrequencies());

		assertEquals(2, fd.get("a"));
		assertEquals(1, fd.get("b"));
		assertEquals(4, fd.get("c"));
	}

	@Test
	public void testGetFrequencySortedEvents() {
		FrequencyDistribution<String> fd = new EntryFrequencyDistribution<String>();

		fd.set("a", 5);
		fd.set("d", 2);
		fd.set("b", 5);
		fd.set("e", 2);
		fd.set("f", 1);
		fd.set("c", 5);

		assertEquals(6, fd.getNumberOfEvents());
		assertEquals(20, fd.getSumOfFrequencies());

		List<PairOfObjectInt<String>> list = fd.getFrequencySortedEvents();

		assertEquals(6, list.size());

		assertEquals("a", list.get(0).getLeftElement());
		assertEquals(5, list.get(0).getRightElement());
		assertEquals("b", list.get(1).getLeftElement());
		assertEquals(5, list.get(1).getRightElement());
		assertEquals("c", list.get(2).getLeftElement());
		assertEquals(5, list.get(2).getRightElement());
		assertEquals("d", list.get(3).getLeftElement());
		assertEquals(2, list.get(3).getRightElement());
		assertEquals("e", list.get(4).getLeftElement());
		assertEquals(2, list.get(4).getRightElement());
		assertEquals("f", list.get(5).getLeftElement());
		assertEquals(1, list.get(5).getRightElement());

		list = fd.getFrequencySortedEvents(4);

		assertEquals(4, list.size());

		assertEquals("a", list.get(0).getLeftElement());
		assertEquals(5, list.get(0).getRightElement());
		assertEquals("b", list.get(1).getLeftElement());
		assertEquals(5, list.get(1).getRightElement());
		assertEquals("c", list.get(2).getLeftElement());
		assertEquals(5, list.get(2).getRightElement());
		assertEquals("d", list.get(3).getLeftElement());
		assertEquals(2, list.get(3).getRightElement());
	}

	@Test
	public void testGetSortedEvents() {
		FrequencyDistribution<String> fd = new EntryFrequencyDistribution<String>();

		fd.set("a", 1);
		fd.set("d", 3);
		fd.set("b", 4);
		fd.set("e", 7);
		fd.set("f", 9);
		fd.set("c", 2);

		assertEquals(6, fd.getNumberOfEvents());
		assertEquals(26, fd.getSumOfFrequencies());

		List<PairOfObjectInt<String>> list = fd.getSortedEvents();

		assertEquals(6, list.size());

		assertEquals("a", list.get(0).getLeftElement());
		assertEquals(1, list.get(0).getRightElement());
		assertEquals("b", list.get(1).getLeftElement());
		assertEquals(4, list.get(1).getRightElement());
		assertEquals("c", list.get(2).getLeftElement());
		assertEquals(2, list.get(2).getRightElement());
		assertEquals("d", list.get(3).getLeftElement());
		assertEquals(3, list.get(3).getRightElement());
		assertEquals("e", list.get(4).getLeftElement());
		assertEquals(7, list.get(4).getRightElement());
		assertEquals("f", list.get(5).getLeftElement());
		assertEquals(9, list.get(5).getRightElement());

		list = fd.getSortedEvents(4);

		assertEquals(4, list.size());

		assertEquals("a", list.get(0).getLeftElement());
		assertEquals(1, list.get(0).getRightElement());
		assertEquals("b", list.get(1).getLeftElement());
		assertEquals(4, list.get(1).getRightElement());
		assertEquals("c", list.get(2).getLeftElement());
		assertEquals(2, list.get(2).getRightElement());
		assertEquals("d", list.get(3).getLeftElement());
		assertEquals(3, list.get(3).getRightElement());
	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(EntryFrequencyDistributionTest.class);
	}
}