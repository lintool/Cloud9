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

import org.junit.Test;

import junit.framework.JUnit4TestAdapter;

public class OpenConditionalFrequencyDistributionOfIntsTest {

	@Test
	public void test1() {
		OpenConditionalFrequencyDistributionOfInts cdf = new OpenConditionalFrequencyDistributionOfInts();

		cdf.set(1, 1, 2);
		cdf.check();

		assertEquals(2, cdf.get(1, 1));
		assertEquals(2, cdf.getSumOfAllFrequencies());

		cdf.set(2, 1, 3);
		cdf.check();

		assertEquals(2, cdf.get(1, 1));
		assertEquals(3, cdf.get(2, 1));
		assertEquals(5, cdf.getSumOfAllFrequencies());

		cdf.set(3, 1, 10);
		cdf.check();

		assertEquals(2, cdf.get(1, 1));
		assertEquals(3, cdf.get(2, 1));
		assertEquals(10, cdf.get(3, 1));
		assertEquals(15, cdf.getSumOfAllFrequencies());

		cdf.set(10, 2, 1);
		cdf.check();

		assertEquals(2, cdf.get(1, 1));
		assertEquals(3, cdf.get(2, 1));
		assertEquals(10, cdf.get(3, 1));
		assertEquals(1, cdf.get(10, 2));
		assertEquals(16, cdf.getSumOfAllFrequencies());

		cdf.set(1, 1, 5);
		cdf.check();

		assertEquals(5, cdf.get(1, 1));
		assertEquals(3, cdf.get(2, 1));
		assertEquals(10, cdf.get(3, 1));
		assertEquals(1, cdf.get(10, 2));
		assertEquals(19, cdf.getSumOfAllFrequencies());
	}

	@Test
	public void test2() {
		OpenConditionalFrequencyDistributionOfInts cdf = new OpenConditionalFrequencyDistributionOfInts();

		cdf.set(1, 1, 2);
		cdf.check();

		assertEquals(2, cdf.get(1, 1));
		assertEquals(2, cdf.getSumOfAllFrequencies());

		cdf.increment(1, 1);
		cdf.check();
		assertEquals(3, cdf.get(1, 1));
		assertEquals(3, cdf.getSumOfAllFrequencies());

		cdf.increment(1, 1, 2);
		cdf.check();
		assertEquals(5, cdf.get(1, 1));
		assertEquals(5, cdf.getSumOfAllFrequencies());

		cdf.increment(2, 1);
		cdf.check();
		assertEquals(5, cdf.get(1, 1));
		assertEquals(1, cdf.get(2, 1));
		assertEquals(6, cdf.getSumOfAllFrequencies());

		cdf.increment(1, 2, 10);
		cdf.check();
		assertEquals(5, cdf.get(1, 1));
		assertEquals(1, cdf.get(2, 1));
		assertEquals(10, cdf.get(1, 2));
		assertEquals(16, cdf.getSumOfAllFrequencies());
	}

	@Test
	public void test3() {
		OpenConditionalFrequencyDistributionOfInts cdf = new OpenConditionalFrequencyDistributionOfInts();

		cdf.set(1, 1, 2);
		cdf.set(1, 2, 5);
		cdf.set(1, 3, 6);
		cdf.set(1, 4, 4);
		cdf.set(2, 1, 3);
		cdf.set(3, 1, 7);
		cdf.check();

		assertEquals(17, cdf.getMarginalCount(1));
		assertEquals(27, cdf.getSumOfAllFrequencies());

		cdf.increment(1, 1, 2);
		cdf.increment(2, 1);

		assertEquals(19, cdf.getMarginalCount(1));
		assertEquals(4, cdf.getMarginalCount(2));
		assertEquals(30, cdf.getSumOfAllFrequencies());
	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(OpenConditionalFrequencyDistributionOfIntsTest.class);
	}
}
