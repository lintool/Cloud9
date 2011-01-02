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

import edu.umd.cloud9.io.PairOfIntLong;

public class OpenLargeFrequencyDistributionOfIntsTest {

	@Test
	public void test1() {
		OpenLargeFrequencyDistributionOfInts fd = new OpenLargeFrequencyDistributionOfInts();

		assertEquals(0, fd.get(1));

		fd.increment(1);
		fd.increment(2);
		fd.increment(3);
		fd.increment(2);
		fd.increment(3);
		fd.increment(3);

		assertEquals(3, fd.getNumberOfEvents());
		assertEquals(6, fd.getSumOfFrequencies());

		assertEquals(1, fd.get(1));
		assertEquals(2, fd.get(2));
		assertEquals(3, fd.get(3));

		fd.decrement(3);

		assertEquals(3, fd.getNumberOfEvents());
		assertEquals(5, fd.getSumOfFrequencies());

		assertEquals(1, fd.get(1));
		assertEquals(2, fd.get(2));
		assertEquals(2, fd.get(3));

		fd.decrement(1);

		assertEquals(2, fd.getNumberOfEvents());
		assertEquals(4, fd.getSumOfFrequencies());

		assertEquals(0, fd.get(1));
		assertEquals(2, fd.get(2));
		assertEquals(2, fd.get(3));
	}

	@Test
	public void test2() {
		OpenLargeFrequencyDistributionOfInts fd = new OpenLargeFrequencyDistributionOfInts();

		fd.increment(1);
		fd.increment(1);
		fd.increment(2);
		fd.increment(3);

		assertEquals(3, fd.getNumberOfEvents());
		assertEquals(4, fd.getSumOfFrequencies());

		assertEquals(2, fd.get(1));
		assertEquals(1, fd.get(2));
		assertEquals(1, fd.get(3));

		fd.set(4, 5);

		assertEquals(4, fd.getNumberOfEvents());
		assertEquals(9, fd.getSumOfFrequencies());

		assertEquals(2, fd.get(1));
		assertEquals(1, fd.get(2));
		assertEquals(1, fd.get(3));
		assertEquals(5, fd.get(4));

		fd.set(1, 5);

		assertEquals(4, fd.getNumberOfEvents());
		assertEquals(12, fd.getSumOfFrequencies());

		assertEquals(5, fd.get(1));
		assertEquals(1, fd.get(2));
		assertEquals(1, fd.get(3));
		assertEquals(5, fd.get(4));

		fd.increment(3);
		fd.increment(3);
		fd.increment(3);

		assertEquals(4, fd.getNumberOfEvents());
		assertEquals(15, fd.getSumOfFrequencies());

		assertEquals(5, fd.get(1));
		assertEquals(1, fd.get(2));
		assertEquals(4, fd.get(3));
		assertEquals(5, fd.get(4));

		fd.set(3, 1);

		assertEquals(4, fd.getNumberOfEvents());
		assertEquals(12, fd.getSumOfFrequencies());

		assertEquals(5, fd.get(1));
		assertEquals(1, fd.get(2));
		assertEquals(1, fd.get(3));
		assertEquals(5, fd.get(4));
	}

	@Test
	public void test3() {
		LargeFrequencyDistributionOfInts fd = new OpenLargeFrequencyDistributionOfInts();

		fd.increment(1);
		fd.increment(1);
		fd.increment(2);
		fd.increment(3);

		assertEquals(3, fd.getNumberOfEvents());
		assertEquals(4, fd.getSumOfFrequencies());

		assertEquals(2, fd.get(1));
		assertEquals(1, fd.get(2));
		assertEquals(1, fd.get(3));

		fd.clear();
		assertEquals(0, fd.getNumberOfEvents());
		assertEquals(0, fd.getSumOfFrequencies());
	}

	@Test(expected = RuntimeException.class)
	public void testFailedDecrement1() {
		OpenLargeFrequencyDistributionOfInts fd = new OpenLargeFrequencyDistributionOfInts();

		fd.increment(1);

		assertEquals(1, fd.getNumberOfEvents());
		assertEquals(1, fd.getSumOfFrequencies());
		assertEquals(1, fd.get(1));

		fd.decrement(1);

		assertEquals(0, fd.getNumberOfEvents());
		assertEquals(0, fd.getSumOfFrequencies());
		assertEquals(0, fd.get(1));

		fd.decrement(1);
	}

	@Test(expected = RuntimeException.class)
	public void testFailedDecrement2() {
		OpenLargeFrequencyDistributionOfInts fd = new OpenLargeFrequencyDistributionOfInts();

		fd.increment(1, 1000);

		assertEquals(1, fd.getNumberOfEvents());
		assertEquals(1000, fd.getSumOfFrequencies());
		assertEquals(1000, fd.get(1));

		fd.decrement(1, 997);

		assertEquals(1, fd.getNumberOfEvents());
		assertEquals(3, fd.getSumOfFrequencies());
		assertEquals(3, fd.get(1));

		fd.decrement(1, 3);

		assertEquals(0, fd.getNumberOfEvents());
		assertEquals(0, fd.getSumOfFrequencies());
		assertEquals(0, fd.get(1));

		fd.increment(1, 3);
		fd.decrement(1, 4);
	}

	@Test
	public void testMultiIncrementDecrement() {
		OpenLargeFrequencyDistributionOfInts fd = new OpenLargeFrequencyDistributionOfInts();

		fd.increment(1, 2);
		fd.increment(2, 3);
		fd.increment(3, 4);

		assertEquals(3, fd.getNumberOfEvents());
		assertEquals(9, fd.getSumOfFrequencies());

		assertEquals(2, fd.get(1));
		assertEquals(3, fd.get(2));
		assertEquals(4, fd.get(3));

		fd.decrement(2, 2);

		assertEquals(3, fd.getNumberOfEvents());
		assertEquals(7, fd.getSumOfFrequencies());

		assertEquals(2, fd.get(1));
		assertEquals(1, fd.get(2));
		assertEquals(4, fd.get(3));
	}

	@Test
	public void testGetFrequencySortedEvents() {
		OpenLargeFrequencyDistributionOfInts fd = new OpenLargeFrequencyDistributionOfInts();

		fd.set(1, 5);
		fd.set(4, 2);
		fd.set(2, 5);
		fd.set(5, 2);
		fd.set(6, 1);
		fd.set(3, 5);

		assertEquals(6, fd.getNumberOfEvents());
		assertEquals(20, fd.getSumOfFrequencies());

		List<PairOfIntLong> list = fd.getFrequencySortedEvents();

		assertEquals(6, list.size());

		assertEquals(1, list.get(0).getLeftElement());
		assertEquals(5, list.get(0).getRightElement());
		assertEquals(2, list.get(1).getLeftElement());
		assertEquals(5, list.get(1).getRightElement());
		assertEquals(3, list.get(2).getLeftElement());
		assertEquals(5, list.get(2).getRightElement());
		assertEquals(4, list.get(3).getLeftElement());
		assertEquals(2, list.get(3).getRightElement());
		assertEquals(5, list.get(4).getLeftElement());
		assertEquals(2, list.get(4).getRightElement());
		assertEquals(6, list.get(5).getLeftElement());
		assertEquals(1, list.get(5).getRightElement());

		list = fd.getFrequencySortedEvents(4);

		assertEquals(4, list.size());

		assertEquals(1, list.get(0).getLeftElement());
		assertEquals(5, list.get(0).getRightElement());
		assertEquals(2, list.get(1).getLeftElement());
		assertEquals(5, list.get(1).getRightElement());
		assertEquals(3, list.get(2).getLeftElement());
		assertEquals(5, list.get(2).getRightElement());
		assertEquals(4, list.get(3).getLeftElement());
		assertEquals(2, list.get(3).getRightElement());
	}

	@Test
	public void testGetSortedEvents() {
		OpenLargeFrequencyDistributionOfInts fd = new OpenLargeFrequencyDistributionOfInts();

		fd.set(1, 1);
		fd.set(4, 3);
		fd.set(2, 4);
		fd.set(5, 7);
		fd.set(6, 9);
		fd.set(3, 2);

		assertEquals(6, fd.getNumberOfEvents());
		assertEquals(26, fd.getSumOfFrequencies());

		List<PairOfIntLong> list = fd.getSortedEvents();

		assertEquals(6, list.size());

		assertEquals(1, list.get(0).getLeftElement());
		assertEquals(1, list.get(0).getRightElement());
		assertEquals(2, list.get(1).getLeftElement());
		assertEquals(4, list.get(1).getRightElement());
		assertEquals(3, list.get(2).getLeftElement());
		assertEquals(2, list.get(2).getRightElement());
		assertEquals(4, list.get(3).getLeftElement());
		assertEquals(3, list.get(3).getRightElement());
		assertEquals(5, list.get(4).getLeftElement());
		assertEquals(7, list.get(4).getRightElement());
		assertEquals(6, list.get(5).getLeftElement());
		assertEquals(9, list.get(5).getRightElement());

		list = fd.getSortedEvents(4);

		assertEquals(4, list.size());

		assertEquals(1, list.get(0).getLeftElement());
		assertEquals(1, list.get(0).getRightElement());
		assertEquals(2, list.get(1).getLeftElement());
		assertEquals(4, list.get(1).getRightElement());
		assertEquals(3, list.get(2).getLeftElement());
		assertEquals(2, list.get(2).getRightElement());
		assertEquals(4, list.get(3).getLeftElement());
		assertEquals(3, list.get(3).getRightElement());
	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(OpenLargeFrequencyDistributionOfIntsTest.class);
	}
}
