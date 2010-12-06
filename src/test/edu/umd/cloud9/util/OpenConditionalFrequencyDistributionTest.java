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

public class OpenConditionalFrequencyDistributionTest {

	@Test
	public void test1() {
		OpenConditionalFrequencyDistribution<String> cdf = new OpenConditionalFrequencyDistribution<String>();

		cdf.set("a", "a", 2);
		cdf.check();

		assertEquals(2, cdf.get("a", "a"));
		assertEquals(2, cdf.getSumOfAllFrequencies());

		cdf.set("b", "a", 3);
		cdf.check();

		assertEquals(2, cdf.get("a", "a"));
		assertEquals(3, cdf.get("b", "a"));
		assertEquals(5, cdf.getSumOfAllFrequencies());

		cdf.set("c", "a", 10);
		cdf.check();

		assertEquals(2, cdf.get("a", "a"));
		assertEquals(3, cdf.get("b", "a"));
		assertEquals(10, cdf.get("c", "a"));
		assertEquals(15, cdf.getSumOfAllFrequencies());

		cdf.set("x", "b", 1);
		cdf.check();

		assertEquals(2, cdf.get("a", "a"));
		assertEquals(3, cdf.get("b", "a"));
		assertEquals(10, cdf.get("c", "a"));
		assertEquals(1, cdf.get("x", "b"));
		assertEquals(16, cdf.getSumOfAllFrequencies());

		cdf.set("a", "a", 5);
		cdf.check();

		assertEquals(5, cdf.get("a", "a"));
		assertEquals(3, cdf.get("b", "a"));
		assertEquals(10, cdf.get("c", "a"));
		assertEquals(1, cdf.get("x", "b"));
		assertEquals(19, cdf.getSumOfAllFrequencies());
	}

	@Test
	public void test2() {
		OpenConditionalFrequencyDistribution<String> cdf = new OpenConditionalFrequencyDistribution<String>();

		cdf.set("a", "a", 2);
		cdf.check();

		assertEquals(2, cdf.get("a", "a"));
		assertEquals(2, cdf.getSumOfAllFrequencies());

		cdf.increment("a", "a");
		cdf.check();
		assertEquals(3, cdf.get("a", "a"));
		assertEquals(3, cdf.getSumOfAllFrequencies());

		cdf.increment("a", "a", 2);
		cdf.check();
		assertEquals(5, cdf.get("a", "a"));
		assertEquals(5, cdf.getSumOfAllFrequencies());

		cdf.increment("b", "a");
		cdf.check();
		assertEquals(5, cdf.get("a", "a"));
		assertEquals(1, cdf.get("b", "a"));
		assertEquals(6, cdf.getSumOfAllFrequencies());

		cdf.increment("a", "b", 10);
		cdf.check();
		assertEquals(5, cdf.get("a", "a"));
		assertEquals(1, cdf.get("b", "a"));
		assertEquals(10, cdf.get("a", "b"));
		assertEquals(16, cdf.getSumOfAllFrequencies());
	}

	@Test
	public void test3() {
		OpenConditionalFrequencyDistribution<String> cdf = new OpenConditionalFrequencyDistribution<String>();

		cdf.set("a", "a", 2);
		cdf.set("a", "b", 5);
		cdf.set("a", "c", 6);
		cdf.set("a", "d", 4);
		cdf.set("b", "a", 3);
		cdf.set("c", "a", 7);
		cdf.check();

		assertEquals(17, cdf.getMarginalCount("a"));
		assertEquals(27, cdf.getSumOfAllFrequencies());

		cdf.increment("a", "a", 2);
		cdf.increment("b", "a");

		assertEquals(19, cdf.getMarginalCount("a"));
		assertEquals(4, cdf.getMarginalCount("b"));
		assertEquals(30, cdf.getSumOfAllFrequencies());
	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(OpenConditionalFrequencyDistributionTest.class);
	}
}
